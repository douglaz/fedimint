pub mod error;
pub mod http;
pub mod iroh;
pub mod metrics;
#[cfg(all(feature = "tor", not(target_family = "wasm")))]
pub mod tor;
pub mod ws;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::{self, Debug};
use std::net::SocketAddr;
use std::pin::Pin;
use std::str::FromStr as _;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, anyhow, bail};
use async_trait::async_trait;
use fedimint_core::envs::{
    FM_WS_API_CONNECT_OVERRIDES_ENV, is_running_in_test_env, parse_kv_list_from_env,
};
use fedimint_core::module::{ApiMethod, ApiRequestErased};
use fedimint_core::util::backoff_util::{FibonacciBackoff, custom_backoff};
use fedimint_core::util::{FmtCompact, FmtCompactAnyhow, SafeUrl};
use fedimint_core::{apply, async_trait_maybe_send};
use fedimint_logging::{LOG_CLIENT_NET_API, LOG_NET};
use fedimint_metrics::HistogramExt as _;
use reqwest::Method;
use serde_json::Value;
use tokio::sync::{OnceCell, SetOnce, broadcast, watch};
use tracing::trace;

use crate::error::ServerError;
use crate::metrics::{CONNECTION_ATTEMPTS_TOTAL, CONNECTION_DURATION_SECONDS};
use crate::ws::WebsocketConnector;

const IROH_NEXT_PATH: &str = "/v1";

/// Parse an advertised Iroh 1.0 endpoint ID into its API URL.
///
/// The `/v1` path is an internal transport-selection marker. It prevents the
/// connector from attempting Iroh 0.35 against an Iroh 1.0-only identity,
/// avoiding both an inappropriate connection attempt and its overhead.
pub fn iroh_next_endpoint_url(endpoint: &str) -> anyhow::Result<SafeUrl> {
    let endpoint_id =
        iroh_next::EndpointId::from_str(endpoint).context("Invalid Iroh 1.0 endpoint ID")?;
    SafeUrl::parse(&format!("iroh://{endpoint_id}{IROH_NEXT_PATH}"))
        .context("Invalid Iroh 1.0 endpoint URL")
}

fn is_iroh_next_endpoint_url(url: &SafeUrl) -> anyhow::Result<bool> {
    match url.path() {
        "" | "/" => Ok(false),
        IROH_NEXT_PATH => Ok(true),
        path => bail!("Unsupported Iroh API URL path: {path}"),
    }
}

fn preserve_iroh_next_marker(original: &SafeUrl, replacement: &SafeUrl) -> SafeUrl {
    // An Iroh-to-Iroh override changes the destination, not the selected wire
    // version. Cross-protocol overrides deliberately replace the whole route.
    if original.scheme() == "iroh"
        && original.path() == IROH_NEXT_PATH
        && replacement.scheme() == "iroh"
    {
        let mut replacement = replacement.clone().to_unsafe();
        replacement.set_path(IROH_NEXT_PATH);
        replacement.into()
    } else {
        replacement.clone()
    }
}

pub type ServerResult<T> = Result<T, ServerError>;

/// Type for connector initialization functions
type ConnectorInitFn = Arc<
    dyn Fn() -> Pin<Box<dyn Future<Output = anyhow::Result<DynConnector>> + Send>> + Send + Sync,
>;

/// Builder for [`ConnectorRegistry`]
///
/// See [`ConnectorRegistry::build_from_client_env`] and similar
/// to create.
#[derive(Debug, Clone)]
#[allow(clippy::struct_excessive_bools)] // Shut up, Clippy
pub struct ConnectorRegistryBuilder {
    /// List of overrides to use when attempting to connect to given url
    ///
    /// This is useful for testing, or forcing non-default network
    /// connectivity.
    connection_overrides: BTreeMap<SafeUrl, SafeUrl>,

    /// Enable Iroh endpoints at all?
    iroh_enable: bool,
    /// Override the Iroh DNS server to use
    iroh_dns: Option<SafeUrl>,
    /// Enable Pkarr DHT discovery
    iroh_pkarr_dht: bool,
    /// Enable compatible iroh-next endpoint preference from guardian metadata
    iroh_next: bool,

    /// Enable Websocket API handling at all?
    ws_enable: bool,
    ws_force_tor: bool,

    // Enable HTTP
    http_enable: bool,
}

impl ConnectorRegistryBuilder {
    #[allow(clippy::unused_async)] // Leave room for async in the future
    pub async fn bind(self) -> anyhow::Result<ConnectorRegistry> {
        let iroh_next = self.iroh_next && self.iroh_enable;

        // Create initialization functions for each connector type
        let mut connectors_lazy: BTreeMap<String, (ConnectorInitFn, OnceCell<DynConnector>)> =
            BTreeMap::new();

        // Eagerly created so consumers can subscribe before the Iroh
        // connector is lazily initialized. Only Iroh bumps it today
        // (on transport-level path changes like relay → direct).
        let path_change = Arc::new(watch::channel(0u64).0);

        // WS connector init function
        let builder_ws = self.clone();
        let ws_connector_init = Arc::new(move || {
            let builder = builder_ws.clone();
            Box::pin(async move { builder.build_ws_connector().await })
                as Pin<Box<dyn Future<Output = anyhow::Result<DynConnector>> + Send>>
        });
        connectors_lazy.insert("ws".into(), (ws_connector_init.clone(), OnceCell::new()));
        connectors_lazy.insert("wss".into(), (ws_connector_init.clone(), OnceCell::new()));

        // Iroh connector init function
        let builder_iroh = self.clone();
        let path_change_iroh = path_change.clone();
        connectors_lazy.insert(
            "iroh".into(),
            (
                Arc::new(move || {
                    let builder = builder_iroh.clone();
                    let path_change = path_change_iroh.clone();
                    Box::pin(async move { builder.build_iroh_connector(path_change).await })
                        as Pin<Box<dyn Future<Output = anyhow::Result<DynConnector>> + Send>>
                }),
                OnceCell::new(),
            ),
        );

        let builder_http = self.clone();
        let http_connector_init = Arc::new(move || {
            let builder = builder_http.clone();
            Box::pin(async move { builder.build_http_connector() })
                as Pin<Box<dyn Future<Output = anyhow::Result<DynConnector>> + Send>>
        });

        connectors_lazy.insert(
            "http".into(),
            (http_connector_init.clone(), OnceCell::new()),
        );
        connectors_lazy.insert(
            "https".into(),
            (http_connector_init.clone(), OnceCell::new()),
        );

        Ok(ConnectorRegistry {
            inner: ConnectorRegistryInner {
                connectors_lazy,
                connection_overrides: self.connection_overrides,
                initialized: SetOnce::new(),
                path_change,
                iroh_next,
            }
            .into(),
        })
    }

    pub async fn build_iroh_connector(
        &self,
        path_change: Arc<watch::Sender<u64>>,
    ) -> anyhow::Result<DynConnector> {
        if !self.iroh_enable {
            bail!("Iroh connector not enabled");
        }
        Ok(Arc::new(
            iroh::IrohConnector::new(self.iroh_dns.clone(), self.iroh_pkarr_dht, path_change)
                .await?,
        ) as DynConnector)
    }

    pub async fn build_ws_connector(&self) -> anyhow::Result<DynConnector> {
        if !self.ws_enable {
            bail!("Websocket connector not enabled");
        }

        match self.ws_force_tor {
            #[cfg(all(feature = "tor", not(target_family = "wasm")))]
            true => {
                use crate::tor::TorConnector;

                Ok(Arc::new(TorConnector::bootstrap().await?) as DynConnector)
            }

            false => Ok(Arc::new(WebsocketConnector::new()) as DynConnector),
            #[allow(unreachable_patterns)]
            _ => bail!("Tor requested, but not support not compiled in"),
        }
    }

    pub fn build_http_connector(&self) -> anyhow::Result<DynConnector> {
        if !self.http_enable {
            bail!("Http connector not enabled");
        }

        Ok(Arc::new(crate::http::HttpConnector::default()) as DynConnector)
    }

    pub fn iroh_pkarr_dht(self, enable: bool) -> Self {
        Self {
            iroh_pkarr_dht: enable,
            ..self
        }
    }

    /// Enable use of compatible iroh-next endpoints advertised in guardian
    /// metadata.
    pub fn iroh_next(self, enable: bool) -> Self {
        Self {
            iroh_next: enable,
            ..self
        }
    }

    pub fn ws_force_tor(self, enable: bool) -> Self {
        Self {
            ws_force_tor: enable,
            ..self
        }
    }

    pub fn http(self, enable: bool) -> Self {
        Self {
            http_enable: enable,
            ..self
        }
    }

    pub fn set_iroh_dns(self, url: SafeUrl) -> Self {
        Self {
            iroh_dns: Some(url),
            ..self
        }
    }

    /// Apply overrides from env variables
    pub fn with_env_var_overrides(mut self) -> anyhow::Result<Self> {
        // TODO: read rest of the env
        for (k, v) in parse_kv_list_from_env::<_, SafeUrl>(FM_WS_API_CONNECT_OVERRIDES_ENV)? {
            self = self.with_connection_override(k, v);
        }

        // Disable iroh-next endpoint preference in test/devimint environments
        // where iroh-next server endpoints are not running.
        if is_running_in_test_env() {
            self.iroh_next = false;
        }

        Ok(Self { ..self })
    }

    pub fn with_connection_override(
        mut self,
        original_url: SafeUrl,
        replacement_url: SafeUrl,
    ) -> Self {
        self.connection_overrides
            .insert(original_url, replacement_url);
        self
    }
}

/// Actual data shared between copies of [`ConnectorRegistry`] handle
struct ConnectorRegistryInner {
    /// Lazily initialized [`Connector`]s per protocol supported
    connectors_lazy: BTreeMap<String, (ConnectorInitFn, OnceCell<DynConnector>)>,
    /// Connection URL overrides for testing/custom routing
    connection_overrides: BTreeMap<SafeUrl, SafeUrl>,
    /// Set on first connection attempt
    ///
    /// This is used for functionality that wants to avoid making
    /// network connections if nothing else did network request.
    initialized: tokio::sync::SetOnce<()>,
    /// Ticks whenever a connector observes a transport-level path change
    /// (e.g. iroh relay → direct). Only Iroh bumps this today.
    path_change: Arc<watch::Sender<u64>>,
    /// Whether compatible iroh-next endpoints advertised in guardian metadata
    /// are used.
    iroh_next: bool,
}

/// A set of available connectivity protocols a client can use to make
/// network API requests (typically to federation).
///
/// Maps from connection URL schema to [`Connector`] to use to connect to it.
///
/// See [`ConnectorRegistry::build_from_client_env`] and similar
/// to create.
///
/// [`ConnectorRegistry::connect_guardian`] is the main entry point for making
/// mixed-networking stack connection.
///
/// Responsibilities:
#[derive(Clone)]
pub struct ConnectorRegistry {
    inner: Arc<ConnectorRegistryInner>,
}

impl fmt::Debug for ConnectorRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectorRegistry")
            .field("connectors_lazy", &self.inner.connectors_lazy.len())
            .field("connection_overrides", &self.inner.connection_overrides)
            .field("iroh_next", &self.inner.iroh_next)
            .finish()
    }
}

impl ConnectorRegistry {
    /// Whether compatible iroh-next endpoints advertised in guardian metadata
    /// are used.
    pub fn iroh_next_enabled(&self) -> bool {
        self.inner.iroh_next
    }

    /// Create a builder with recommended defaults intended for client-side
    /// usage
    ///
    /// In particular mobile devices are considered.
    pub fn build_from_client_defaults() -> ConnectorRegistryBuilder {
        ConnectorRegistryBuilder {
            iroh_enable: true,
            iroh_dns: None,
            iroh_pkarr_dht: false,
            iroh_next: true,
            ws_enable: true,
            ws_force_tor: false,
            http_enable: true,

            connection_overrides: BTreeMap::default(),
        }
    }

    /// Create a builder with recommended defaults intended for the server-side
    /// usage
    pub fn build_from_server_defaults() -> ConnectorRegistryBuilder {
        ConnectorRegistryBuilder {
            iroh_enable: true,
            iroh_dns: None,
            iroh_pkarr_dht: true,
            iroh_next: true,
            ws_enable: true,
            ws_force_tor: false,
            http_enable: false,

            connection_overrides: BTreeMap::default(),
        }
    }

    /// Create a builder with recommended defaults intended for testing
    /// usage
    pub fn build_from_testing_defaults() -> ConnectorRegistryBuilder {
        ConnectorRegistryBuilder {
            iroh_enable: true,
            iroh_dns: None,
            iroh_pkarr_dht: false,
            iroh_next: false,
            ws_enable: true,
            ws_force_tor: false,
            http_enable: true,

            connection_overrides: BTreeMap::default(),
        }
    }

    /// Like [`Self::build_from_client_defaults`] build will apply
    /// environment-provided overrides.
    pub fn build_from_client_env() -> anyhow::Result<ConnectorRegistryBuilder> {
        let builder = Self::build_from_client_defaults().with_env_var_overrides()?;
        Ok(builder)
    }

    /// Like [`Self::build_from_server_defaults`] build will apply
    /// environment-provided overrides.
    pub fn build_from_server_env() -> anyhow::Result<ConnectorRegistryBuilder> {
        let builder = Self::build_from_server_defaults().with_env_var_overrides()?;
        Ok(builder)
    }

    /// Like [`Self::build_from_testing_defaults`] build will apply
    /// environment-provided overrides.
    pub fn build_from_testing_env() -> anyhow::Result<ConnectorRegistryBuilder> {
        let builder = Self::build_from_testing_defaults().with_env_var_overrides()?;
        Ok(builder)
    }

    /// Wait until some connections have been made
    pub async fn wait_for_initialized_connections(&self) {
        self.inner.initialized.wait().await;
    }

    /// Connect to a given `url` using matching [`Connector`]
    ///
    /// This is the main function consumed by the downstream use for making
    /// connection.
    pub async fn connect_guardian(
        &self,
        url: &SafeUrl,
        api_secret: Option<&str>,
    ) -> ServerResult<DynGuaridianConnection> {
        trace!(
            target: LOG_NET,
            %url,
            "Connection requested to guardian"
        );
        let _ = self.inner.initialized.set(());

        let replacement = self
            .inner
            .connection_overrides
            .get(url)
            .map(|replacement| preserve_iroh_next_marker(url, replacement));
        let url = match replacement.as_ref() {
            Some(replacement) => {
                trace!(
                    target: LOG_NET,
                    original_url = %url,
                    replacement_url = %replacement,
                    "Using a connectivity override for connection"
                );

                replacement
            }
            None => url,
        };

        let scheme = url.scheme().to_string();

        let Some(connector_lazy) = self.inner.connectors_lazy.get(&scheme) else {
            return Err(ServerError::InvalidEndpoint(anyhow!(
                "Unsupported scheme: {}; missing endpoint handler",
                url.scheme()
            )));
        };

        // Clone the init function to use in the async block
        let init_fn = connector_lazy.0.clone();

        let timer = CONNECTION_DURATION_SECONDS
            .with_label_values(&[&scheme])
            .start_timer_ext();

        let result = connector_lazy
            .1
            .get_or_try_init(|| async move { init_fn().await })
            .await
            .map_err(|e| {
                ServerError::Transport(anyhow!(
                    "Connector failed to initialize: {}",
                    e.fmt_compact_anyhow()
                ))
            })?
            .connect_guardian(url, api_secret)
            .await;

        timer.observe_duration();

        let result_label = if result.is_ok() { "success" } else { "error" }.to_string();
        CONNECTION_ATTEMPTS_TOTAL
            .with_label_values(&[&scheme, &result_label])
            .inc();

        let conn = result.inspect_err(|err| {
            trace!(
                target: LOG_NET,
                %url,
                err = %err.fmt_compact(),
                "Connection failed"
            );
        })?;

        trace!(
            target: LOG_NET,
            %url,
            "Connection returned"
        );
        Ok(conn)
    }

    /// Connect to a given `url` using matching [`Connector`] to a gateway
    ///
    /// This is the main function consumed by the downstream use for making
    /// connection.
    pub async fn connect_gateway(&self, url: &SafeUrl) -> anyhow::Result<DynGatewayConnection> {
        trace!(
            target: LOG_NET,
            %url,
            "Connection requested to gateway"
        );
        let _ = self.inner.initialized.set(());

        let url = match self.inner.connection_overrides.get(url) {
            Some(replacement) => {
                trace!(
                    target: LOG_NET,
                    original_url = %url,
                    replacement_url = %replacement,
                    "Using a connectivity override for connection"
                );

                replacement
            }
            None => url,
        };

        let scheme = url.scheme().to_string();

        let Some(connector_lazy) = self.inner.connectors_lazy.get(&scheme) else {
            return Err(anyhow!(
                "Unsupported scheme: {}; missing endpoint handler",
                url.scheme()
            ));
        };

        // Clone the init function to use in the async block
        let init_fn = connector_lazy.0.clone();

        let timer = CONNECTION_DURATION_SECONDS
            .with_label_values(&[&scheme])
            .start_timer_ext();

        let result = connector_lazy
            .1
            .get_or_try_init(|| async move { init_fn().await })
            .await
            .map_err(|e| {
                ServerError::Transport(anyhow!(
                    "Connector failed to initialize: {}",
                    e.fmt_compact_anyhow()
                ))
            })?
            .connect_gateway(url)
            .await;

        timer.observe_duration();

        let result_label = if result.is_ok() { "success" } else { "error" }.to_string();
        CONNECTION_ATTEMPTS_TOTAL
            .with_label_values(&[&scheme, &result_label])
            .inc();

        result
    }

    /// Report how a connection to `url` is currently reaching its peer.
    ///
    /// Returns [`Connectivity::Unknown`] if no connector for the url's scheme
    /// is registered, or if the matching connector has not been initialized
    /// yet (i.e. no connection attempt has been made).
    pub fn connectivity(&self, url: &SafeUrl) -> Connectivity {
        let url = match self.inner.connection_overrides.get(url) {
            Some(replacement) => replacement,
            None => url,
        };

        let Some((_, connector_cell)) = self.inner.connectors_lazy.get(url.scheme()) else {
            return Connectivity::Unknown;
        };

        match connector_cell.get() {
            Some(connector) => connector.connectivity(url),
            None => Connectivity::Unknown,
        }
    }

    /// Return iroh-specific peer details if `url` is handled by the iroh
    /// connector.
    pub async fn iroh_peer_info(
        &self,
        url: &SafeUrl,
        path_timeout: Duration,
    ) -> ServerResult<Option<IrohPeerInfo>> {
        let url = match self.inner.connection_overrides.get(url) {
            Some(replacement) => replacement,
            None => url,
        };

        let Some((init_fn, connector_cell)) = self.inner.connectors_lazy.get(url.scheme()) else {
            return Ok(None);
        };

        let init_fn = init_fn.clone();
        connector_cell
            .get_or_try_init(|| async move { init_fn().await })
            .await
            .map_err(|e| {
                ServerError::Transport(anyhow!(
                    "Connector failed to initialize: {}",
                    e.fmt_compact_anyhow()
                ))
            })?
            .iroh_peer_info(url, path_timeout)
            .await
    }

    /// Subscribe to transport-level connectivity changes across all
    /// connectors managed by this registry.
    ///
    /// The receiver ticks whenever a connector observes a path change on
    /// an existing connection (for example an iroh connection upgrading
    /// from relay to direct). The carried `u64` is an opaque counter —
    /// consumers should treat each update as a "re-read connectivity"
    /// signal.
    pub fn connectivity_change_notifier(&self) -> watch::Receiver<u64> {
        self.inner.path_change.subscribe()
    }

    /// Signal that connectivity should be re-read, without a connector
    /// having observed a path change on an existing connection.
    ///
    /// A newly established connection is such a case, and it is not
    /// self-announcing: the connectors log the path they start on but only
    /// tick this notifier on subsequent *changes*. A replacement connection
    /// can therefore come up on a different path (relay where the old one
    /// was direct, or the reverse) with nothing telling consumers to look
    /// again.
    ///
    /// This is deliberately not a membership signal. A deliberate refresh
    /// keeps its peer advertised throughout, so the active set does not
    /// change and must not be made to tick — that is what would put the
    /// `Disconnected` flap back.
    pub(crate) fn note_path_change(&self) {
        self.inner
            .path_change
            .send_modify(|c| *c = c.wrapping_add(1));
    }
}
pub type DynConnector = Arc<dyn Connector>;

#[async_trait]
pub trait Connector: Send + Sync + 'static + Debug {
    async fn connect_guardian(
        &self,
        url: &SafeUrl,
        api_secret: Option<&str>,
    ) -> ServerResult<DynGuaridianConnection>;

    async fn connect_gateway(&self, url: &SafeUrl) -> anyhow::Result<DynGatewayConnection>;

    /// Report how a connection to `url` is currently reaching its peer.
    fn connectivity(&self, url: &SafeUrl) -> Connectivity;

    /// Return iroh-specific peer details if this connector supports them.
    async fn iroh_peer_info(
        &self,
        _url: &SafeUrl,
        _path_timeout: Duration,
    ) -> ServerResult<Option<IrohPeerInfo>> {
        Ok(None)
    }
}

/// How a connection is currently reaching its peer.
///
/// Transports without a relay concept (WS, HTTP) are always
/// [`Connectivity::Direct`]. Tor-routed connections report
/// [`Connectivity::Tor`]. Iroh connections may be [`Connectivity::Direct`]
/// (peer-to-peer), [`Connectivity::Relay`] (routed through a relay
/// server), or [`Connectivity::Mixed`] (both paths active); for Iroh this
/// can change at runtime as hole-punching succeeds or falls back.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Connectivity {
    Direct,
    Relay,
    Mixed,
    Tor,
    Unknown,
}

/// Per-peer connection state reported by the federation API.
///
/// [`PeerStatus::Connected`] carries the current [`Connectivity`] of the
/// active connection. Consumers are woken both by pool-membership changes and
/// by [`ConnectorRegistry::connectivity_change_notifier`], which ticks when a
/// connector observes a path change on an existing connection (relay→direct)
/// and when a replacement connection is established, so a path change does not
/// wait on pool membership to surface.
///
/// It can still lag in one case worth naming: the iroh path monitors are only
/// spawned on the connection-override path, so on a plain deployment no
/// in-connection relay→direct upgrade is observed at all and the reported
/// [`Connectivity`] is whatever the connector last resolved.
///
/// At the pool-membership layer, [`PeerStatus::Disconnected`] means the pool
/// holds no live connection to the peer AND is not in the middle of a refresh.
/// A pooled connection that its owner deliberately rotated (see
/// [`ConnectionLiveness::Retired`]) does not surface through that path: the
/// peer stays advertised across the rotation, and only a re-dial that actually
/// fails removes it. The pool dials on demand, so that re-dial is the next
/// request for the peer; a rotation followed by no traffic at all leaves the
/// peer advertised until then. The API status stream may also report
/// `Disconnected` when an advertised connection's [`Connectivity`] is
/// [`Connectivity::Unknown`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PeerStatus {
    Disconnected,
    Connected(Connectivity),
}

/// Iroh-specific reachability details for a guardian endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IrohPeerInfo {
    pub node_id: String,
    pub connectivity: Connectivity,
    pub direct_addr: Option<SocketAddr>,
    pub known_direct_addrs: Vec<SocketAddr>,
    pub relay_url: Option<String>,
}

/// Whether a connection can still serve new requests and, when it cannot,
/// what that says about the peer behind it.
///
/// This is deliberately one three-valued enum rather than a second bool
/// alongside [`IConnection::is_connected`]: two independent bools admit a
/// meaningless state and let a caller test them in the wrong order, and the
/// order matters (see [`ConnectionLiveness::Retired`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConnectionLiveness {
    /// Usable for new requests.
    Live,

    /// Not usable for new requests, but the peer is NOT known to be
    /// unreachable: the connection's owner rotated it on purpose and a
    /// reconnect is expected to succeed. [`ConnectionPool`] therefore keeps
    /// the peer advertised across a retirement instead of announcing a
    /// disconnect it has no evidence for.
    ///
    /// Retirement must stay a rare, self-limiting event. A connection that
    /// reported `Retired` continuously would drive an unthrottled dial loop,
    /// because the pool grants a refresh its first re-dial for free (see
    /// [`ConnectionState::new_refreshing`]). The only producer today is the
    /// iroh long-poll timeout tiers. Their tightest budget is the lnv2 payment
    /// wait at 5 minutes, less up to a minute of per-peer spread, so a given
    /// connection can report this at most about once every four minutes — it
    /// cannot become a hot path. The 60s prompt tier does NOT produce this: a
    /// prompt endpoint failing to answer is a fault, and reports `Dead`.
    ///
    /// "A reconnect is expected" is not a promise that the pool makes one: it
    /// dials on demand, so the re-dial comes from the next caller that wants
    /// this peer (`fedimint_client::Client::spawn_federation_reconnect` is the
    /// opt-in for reconnecting without one).
    Retired,

    /// Not usable, and the peer should be treated as unreachable until a dial
    /// proves otherwise.
    Dead,
}

/// Generic connection trait shared between [`IGuardianConnection`] and
/// [`IGatewayConnection`]
#[apply(async_trait_maybe_send!)]
pub trait IConnection: Debug + Send + Sync + 'static {
    fn is_connected(&self) -> bool;

    /// Three-valued refinement of [`Self::is_connected`], distinguishing a
    /// deliberate rotation from a peer that went away.
    ///
    /// Provided rather than required: a transport with no retirement concept
    /// only ever produces [`ConnectionLiveness::Live`] or
    /// [`ConnectionLiveness::Dead`], which is exactly what the default
    /// derives from [`Self::is_connected`].
    ///
    /// Implementors must keep `liveness() == Live` equivalent to
    /// `is_connected()`.
    fn liveness(&self) -> ConnectionLiveness {
        if self.is_connected() {
            ConnectionLiveness::Live
        } else {
            ConnectionLiveness::Dead
        }
    }

    async fn await_disconnection(&self);
}

/// A connection from api client to a federation guardian (type erased)
pub type DynGuaridianConnection = Arc<dyn IGuardianConnection>;

/// A connection from api client to a federation guardian
#[async_trait]
pub trait IGuardianConnection: IConnection + Debug + Send + Sync + 'static {
    async fn request(&self, method: ApiMethod, request: ApiRequestErased) -> ServerResult<Value>;

    fn into_dyn(self) -> DynGuaridianConnection
    where
        Self: Sized,
    {
        Arc::new(self)
    }
}

/// A connection from api client to a gateway (type erased)
pub type DynGatewayConnection = Arc<dyn IGatewayConnection>;

/// A connection from a client to a gateway
#[apply(async_trait_maybe_send!)]
pub trait IGatewayConnection: IConnection + Debug + Send + Sync + 'static {
    async fn request(
        &self,
        password: Option<String>,
        method: Method,
        route: &str,
        payload: Option<Value>,
    ) -> ServerResult<Value>;

    fn into_dyn(self) -> DynGatewayConnection
    where
        Self: Sized,
    {
        Arc::new(self)
    }
}

#[derive(Debug)]
pub struct ConnectionPool<T: IConnection + ?Sized> {
    /// Available connectors which we can make connections
    connectors: ConnectorRegistry,

    active_connections: watch::Sender<BTreeSet<SafeUrl>>,

    /// Connection pool
    ///
    /// Every entry in this map will be created on demand and correspond to a
    /// single outgoing connection to a certain URL that is in the process
    /// of being established, or we already established.
    #[allow(clippy::type_complexity)]
    connections: Arc<tokio::sync::Mutex<HashMap<SafeUrl, Arc<ConnectionState<T>>>>>,
}

impl<T: IConnection + ?Sized> Clone for ConnectionPool<T> {
    fn clone(&self) -> Self {
        Self {
            connectors: self.connectors.clone(),
            connections: self.connections.clone(),
            active_connections: self.active_connections.clone(),
        }
    }
}

impl<T: IConnection + ?Sized> ConnectionPool<T> {
    pub fn new(connectors: ConnectorRegistry) -> Self {
        Self {
            connectors,
            connections: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            active_connections: watch::channel(BTreeSet::new()).0,
        }
    }

    async fn get_or_init_pool_entry(&self, url: &SafeUrl) -> Arc<ConnectionState<T>> {
        let mut pool_locked = self.connections.lock().await;
        pool_locked
            .entry(url.to_owned())
            .and_modify(|entry_arc| {
                // Check if the existing connection can still serve new requests, and if
                // not reset the whole entry.
                //
                // This resets the state (like connectivity backoff), which is what we want.
                // Since the (`OnceCell`) was already initialized, it means connection was
                // successfully before, and stopped being usable afterwards.
                let Some(existing_conn) = entry_arc.connection.get() else {
                    return;
                };
                match existing_conn.liveness() {
                    ConnectionLiveness::Live => {}
                    ConnectionLiveness::Retired => {
                        // A retirement is a rotation the connection's owner chose, not
                        // evidence that the peer went away, so `active_connections` is
                        // deliberately left alone: an idle subscription whose long-poll
                        // budget expires on a schedule must not report its guardian
                        // `Disconnected` every cycle. If the re-dial then genuinely
                        // fails, `settle_active_flag` un-advertises the peer, so a
                        // permanently-down peer is still dropped within one failed dial.
                        trace!(
                            target: LOG_CLIENT_NET_API,
                            %url,
                            "Existing connection was retired, refreshing while the peer stays advertised"
                        );
                        *entry_arc = Arc::new(ConnectionState::new_refreshing());
                    }
                    ConnectionLiveness::Dead => {
                        trace!(
                            target: LOG_CLIENT_NET_API,
                            %url,
                            "Existing connection is disconnected, removing from pool"
                        );
                        self.active_connections.send_modify(|v| {
                            v.remove(url);
                        });
                        *entry_arc = Arc::new(ConnectionState::new_reconnecting());
                    }
                }
            })
            .or_insert_with(|| Arc::new(ConnectionState::new_initial()))
            .clone()
    }

    /// Publish the outcome of a connection attempt made on `attempted` into
    /// `active_connections`, but only if `attempted` is still the pool's
    /// current entry for `url`.
    ///
    /// A pool entry is replaced wholesale on every reset (see
    /// [`Self::get_or_init_pool_entry`]), while tasks already inside
    /// `OnceCell::get_or_try_init` keep running against the `Arc` they
    /// captured. Without the identity check a stale generation's late result
    /// would speak for the current one — in particular a stale *failed* dial
    /// would un-advertise a URL whose current generation is connected and
    /// healthy, and nothing would re-advertise it until that connection died.
    ///
    /// Today's two call sites cannot actually reach that state, and the reason
    /// is worth stating so the check is not mistaken for load-bearing: an entry
    /// is only ever replaced while its `OnceCell` is populated (the reset skips
    /// an empty cell, `get_or_init_pool_entry` above), whereas a dial runs
    /// inside `get_or_try_init`, which holds the initialization permit and sets
    /// the cell only on success — so no swap can happen under a dial in flight.
    /// The check is insurance that keeps the invariant local to this helper
    /// rather than spread across its callers.
    ///
    /// Lock order is pool mutex → `active_connections` watch, matching
    /// [`Self::get_or_init_pool_entry`].
    async fn settle_active_flag(
        &self,
        url: &SafeUrl,
        attempted: &Arc<ConnectionState<T>>,
        connected: bool,
    ) {
        let pool_locked = self.connections.lock().await;

        let Some(current) = pool_locked.get(url) else {
            return;
        };
        if !Arc::ptr_eq(current, attempted) {
            return;
        }
        // Defence in depth for the failure side: if this very generation already
        // holds a connection, another task won the race on it and our failure
        // says nothing about the peer's reachability. (Reached from inside
        // `get_or_try_init` this cannot fire — the cell is only set once the
        // closure returns `Ok` — but the check costs nothing and keeps the
        // helper correct for any caller.)
        if !connected && current.connection.get().is_some() {
            return;
        }

        // `send_if_modified`, NOT `send_modify`: removing a URL that was never
        // advertised (the ordinary first-connect failure) changes nothing, and
        // must not tick every status consumer.
        self.active_connections.send_if_modified(|v| {
            if connected {
                v.insert(url.clone())
            } else {
                v.remove(url)
            }
        });
    }

    pub async fn get_or_create_connection<F, Fut>(
        &self,
        url: &SafeUrl,
        api_secret: Option<&str>,
        create_connection: F,
    ) -> ServerResult<Arc<T>>
    where
        F: Fn(SafeUrl, Option<String>, ConnectorRegistry) -> Fut + Clone + Send + Sync + 'static,
        Fut: Future<Output = ServerResult<Arc<T>>> + Send + 'static,
    {
        let pool_entry_arc = self.get_or_init_pool_entry(url).await;

        let leader_tx = loop {
            let mut leader_rx = {
                let mut chan_locked = pool_entry_arc
                    .merge_connection_attempts_chan
                    .lock()
                    .expect("locking error");

                if chan_locked.is_closed() {
                    let (leader_tx, leader_rx) = broadcast::channel(1);
                    *chan_locked = leader_rx;
                    // whoever was trying to connect last time is gone
                    // we're out of this lame loop for followers
                    break leader_tx;
                }

                // lets piggyback on the existing leader
                chan_locked.resubscribe()
            };

            if let Ok(res) = leader_rx.recv().await {
                match res {
                    Ok(o) => return Ok(o),
                    Err(err) => {
                        return Err(ServerError::Connection(anyhow::format_err!("{}", err)));
                    }
                }
            }
        };

        let conn = pool_entry_arc
            .connection
            .get_or_try_init(|| async {
                let retry_delay = pool_entry_arc.pre_reconnect_delay();
                fedimint_core::runtime::sleep(retry_delay).await;

                trace!(target: LOG_CLIENT_NET_API, %url, "Attempting to create a new connection");
                let res = create_connection(
                    url.clone(),
                    api_secret.map(std::string::ToString::to_string),
                    self.connectors.clone(),
                )
                .await;

                // A failed dial un-advertises the peer, and does so BEFORE the result is
                // broadcast to the followers piggybacking on this attempt. Removing
                // first leaves no window in which a task woken by this very failure
                // could read a stale "connected" for the URL. It is not load-bearing
                // against the "a follower's later success gets overwritten by this
                // removal" hazard, which cannot occur either way: `get_or_try_init`
                // holds its initialization permit for the whole closure, so a woken
                // follower's retry lands strictly after this removal, and an attempt on
                // a later generation is turned away by `settle_active_flag`'s identity
                // check rather than by ordering.
                //
                // The removal itself is what keeps a refresh honest: `active_connections`
                // is only inserted into after a successful `create_connection`, so a
                // refresh that kept the peer advertised (it does) and then failed to
                // re-dial would leave the `OnceCell` empty, every later
                // `get_or_init_pool_entry` would skip its `and_modify` body, and a
                // permanently-down peer would be advertised `Connected` forever.
                if res.is_err() {
                    self.settle_active_flag(url, &pool_entry_arc, false).await;
                }

                // If any other task was also waiting to connect, send them the connection
                // result.
                //
                // Note: we want to send both Ok or Err, so `res?` is used only afterwards.
                let _ = leader_tx.send(
                    res.as_ref()
                        .map(|o| o.clone())
                        .map_err(|err| err.to_string()),
                );

                let conn = res?;

                // Guarded rather than an unconditional insert: an entry that was already
                // replaced (say by a refresh that raced this dial) no longer speaks for
                // the pool, and advertising a connection the pool does not hold would
                // leave an orphan in the active set.
                self.settle_active_flag(url, &pool_entry_arc, true).await;

                // A refresh keeps its peer advertised, so the line above is a no-op
                // across one and no membership tick is emitted — which is the point,
                // and what stops the `Disconnected` flap. But status consumers derive
                // more than membership: they re-read the connector's path
                // (relay/direct) on every tick, and a replacement connection can come
                // up on a different path than the one it replaced. The connectors log
                // their initial path without ticking, so nothing else announces it.
                //
                // Signal "re-read connectivity" here instead. It is the right axis
                // (this is a new path, not a membership change), it cannot resurrect
                // the flap, and it covers every transport rather than just iroh.
                self.connectors.note_path_change();

                // Reconcile `active_connections` once this connection stops serving new
                // requests, so a peer that went away is dropped from the advertised set
                // even if no caller asks for it again.
                //
                // Deliberately a one-shot watch and NOT a re-dial loop: reconnecting a
                // connection nobody asked for is
                // `fedimint_client::Client::spawn_federation_reconnect`'s opt-in job
                // (it costs data and battery, and its tasks are cancellable with the
                // client, unlike this detached one). A retirement therefore reconciles
                // here — vacating the entry while keeping the peer advertised, see
                // [`ConnectionLiveness::Retired`] — and the re-dial comes from the next
                // caller.
                fedimint_core::runtime::spawn("connection disconnect watch", {
                    let conn = conn.clone();
                    let s = self.clone();
                    let url = url.clone();
                    async move {
                        conn.await_disconnection().await;
                        s.get_or_init_pool_entry(&url).await;
                    }
                });

                Ok(conn)
            })
            .await?;

        trace!(target: LOG_CLIENT_NET_API, %url, "Connection ready");
        Ok(conn.clone())
    }
    /// Get receiver for changes in the active connections
    pub fn get_active_connection_receiver(&self) -> watch::Receiver<BTreeSet<SafeUrl>> {
        self.active_connections.subscribe()
    }

    pub async fn wait_for_initialized_connections(&self) {
        self.connectors.wait_for_initialized_connections().await
    }

    /// Report how a connection to `url` is currently reaching its peer.
    pub fn connectivity(&self, url: &SafeUrl) -> Connectivity {
        self.connectors.connectivity(url)
    }

    /// Subscribe to transport-level connectivity changes observed by
    /// the connectors underlying this pool.
    pub fn connectivity_change_notifier(&self) -> watch::Receiver<u64> {
        self.connectors.connectivity_change_notifier()
    }
}

/// Inner part of [`ConnectionState`] preserving state between attempts to
/// initialize [`ConnectionState::connection`]
#[derive(Debug)]
struct ConnectionStateInner {
    fresh: bool,
    backoff: FibonacciBackoff,
}

#[derive(Debug)]
pub struct ConnectionState<T: ?Sized> {
    /// Connection we are trying to or already established
    pub connection: tokio::sync::OnceCell<Arc<T>>,

    /// When tasks attempt to connect at the same time,
    /// this is the receiving end of the channel where
    /// the "leader" sends a result.
    merge_connection_attempts_chan:
        std::sync::Mutex<broadcast::Receiver<std::result::Result<Arc<T>, String>>>,

    /// State that technically is protected every time by
    /// the serialization of `OnceCell::get_or_try_init`, but
    /// for Rust purposes needs to be locked.
    inner: std::sync::Mutex<ConnectionStateInner>,
}

impl<T: ?Sized> ConnectionState<T> {
    /// Create a new connection state for a first time connection
    pub fn new_initial() -> Self {
        Self {
            connection: OnceCell::new(),
            inner: std::sync::Mutex::new(ConnectionStateInner {
                fresh: true,
                backoff: custom_backoff(
                    // First time connections start quick
                    Duration::from_millis(5),
                    Duration::from_secs(30),
                    None,
                ),
            }),
            merge_connection_attempts_chan: std::sync::Mutex::new(broadcast::channel(1).1),
        }
    }

    /// Create a new connection state for a connection that already failed, and
    /// is being reset
    pub fn new_reconnecting() -> Self {
        Self {
            connection: OnceCell::new(),
            inner: std::sync::Mutex::new(ConnectionStateInner {
                // set the attempts to 1, indicating that
                fresh: false,
                backoff: custom_backoff(
                    // Connections after a disconnect start with some minimum delay
                    Duration::from_millis(500),
                    Duration::from_secs(30),
                    None,
                ),
            }),
            merge_connection_attempts_chan: std::sync::Mutex::new(broadcast::channel(1).1),
        }
    }

    /// Create a new connection state for a connection that was *retired* on
    /// purpose (see [`ConnectionLiveness::Retired`]) rather than one that
    /// failed.
    ///
    /// The first re-dial is immediate. [`Self::new_reconnecting`]'s ≥500ms
    /// floor exists for "something is probably wrong, don't hammer", and a
    /// rotation we chose is not that; paying it on every refresh cycle only
    /// keeps the peer's "still connected" claim dishonest for longer than
    /// necessary. Every attempt after the first falls back to the same
    /// post-disconnect backoff, so a peer that genuinely died at a refresh
    /// boundary is still not hammered.
    ///
    /// Deliberately NOT [`Self::new_initial`]: its ~5ms floor would tight-loop
    /// if retirements ever did start repeating.
    pub fn new_refreshing() -> Self {
        Self {
            connection: OnceCell::new(),
            inner: std::sync::Mutex::new(ConnectionStateInner {
                // The rotation was intentional, so the first attempt is free.
                fresh: true,
                backoff: custom_backoff(
                    // Anything past that first attempt looks like a real failure,
                    // so it pays the post-disconnect floor.
                    Duration::from_millis(500),
                    Duration::from_secs(30),
                    None,
                ),
            }),
            merge_connection_attempts_chan: std::sync::Mutex::new(broadcast::channel(1).1),
        }
    }

    /// Record the fact that an attempt to connect is being made, and return
    /// time the caller should wait.
    pub fn pre_reconnect_delay(&self) -> Duration {
        let mut backoff_locked = self.inner.lock().expect("Locking failed");
        let fresh = backoff_locked.fresh;

        backoff_locked.fresh = false;

        if fresh {
            Duration::default()
        } else {
            backoff_locked.backoff.next().expect("Keeps retrying")
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    /// An in-memory stand-in for a pooled connection whose liveness the test
    /// drives directly, so the pool's bookkeeping can be exercised without
    /// opening a socket or standing up an iroh connector.
    #[derive(Debug)]
    struct FakeConn {
        liveness: Mutex<ConnectionLiveness>,
    }

    impl FakeConn {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                liveness: Mutex::new(ConnectionLiveness::Live),
            })
        }

        fn set(&self, liveness: ConnectionLiveness) {
            *self.liveness.lock().expect("locking failed") = liveness;
        }
    }

    #[apply(async_trait_maybe_send!)]
    impl IConnection for FakeConn {
        fn is_connected(&self) -> bool {
            self.liveness() == ConnectionLiveness::Live
        }

        fn liveness(&self) -> ConnectionLiveness {
            *self.liveness.lock().expect("locking failed")
        }

        async fn await_disconnection(&self) {
            // The tests drive reconciliation by calling `get_or_init_pool_entry`
            // themselves; pending here keeps the pool's watch task from racing
            // them.
            std::future::pending().await
        }
    }

    type FakeConnectFut = Pin<Box<dyn Future<Output = ServerResult<Arc<FakeConn>>> + Send>>;

    fn connect_ok(
        conn: Arc<FakeConn>,
    ) -> impl Fn(SafeUrl, Option<String>, ConnectorRegistry) -> FakeConnectFut
    + Clone
    + Send
    + Sync
    + 'static {
        move |_url, _api_secret, _connectors| {
            let conn = conn.clone();
            Box::pin(async move { Ok(conn) })
        }
    }

    fn connect_err() -> impl Fn(SafeUrl, Option<String>, ConnectorRegistry) -> FakeConnectFut
    + Clone
    + Send
    + Sync
    + 'static {
        move |_url, _api_secret, _connectors| {
            Box::pin(async move { Err(ServerError::Connection(anyhow!("dial failed in test"))) })
        }
    }

    /// A connection whose retirement wakes the pool's disconnect watcher, used
    /// to pin what that watcher does — and does not — do without another API
    /// request.
    #[derive(Debug)]
    struct AutoRefreshConn {
        retired: watch::Sender<bool>,
    }

    impl AutoRefreshConn {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                retired: watch::Sender::new(false),
            })
        }

        fn retire(&self) {
            self.retired.send_replace(true);
        }
    }

    #[apply(async_trait_maybe_send!)]
    impl IConnection for AutoRefreshConn {
        fn is_connected(&self) -> bool {
            !*self.retired.borrow()
        }

        fn liveness(&self) -> ConnectionLiveness {
            if *self.retired.borrow() {
                ConnectionLiveness::Retired
            } else {
                ConnectionLiveness::Live
            }
        }

        async fn await_disconnection(&self) {
            let mut rx = self.retired.subscribe();
            let _ = rx.wait_for(|retired| *retired).await;
        }
    }

    type AutoRefreshConnectFut =
        Pin<Box<dyn Future<Output = ServerResult<Arc<AutoRefreshConn>>> + Send>>;

    fn connect_counted(
        conn: Arc<AutoRefreshConn>,
        attempts: Arc<AtomicUsize>,
    ) -> impl Fn(SafeUrl, Option<String>, ConnectorRegistry) -> AutoRefreshConnectFut
    + Clone
    + Send
    + Sync
    + 'static {
        move |_url, _api_secret, _connectors| {
            let conn = conn.clone();
            attempts.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move { Ok(conn) })
        }
    }

    /// A pool over a registry that is never actually used: every test supplies
    /// its own `create_connection`, and the registry's connectors stay lazily
    /// uninitialized, so nothing here touches the network.
    async fn test_pool() -> (ConnectionPool<FakeConn>, SafeUrl) {
        let connectors = ConnectorRegistry::build_from_testing_defaults()
            .bind()
            .await
            .expect("registry builds");
        let url = SafeUrl::parse("ws://guardian.invalid:1234").expect("valid url");
        (ConnectionPool::new(connectors), url)
    }

    fn is_advertised<T: IConnection + ?Sized>(pool: &ConnectionPool<T>, url: &SafeUrl) -> bool {
        pool.active_connections.borrow().contains(url)
    }

    #[tokio::test]
    async fn refresh_retirement_keeps_the_peer_advertised() {
        let (pool, url) = test_pool().await;
        let conn = FakeConn::new();
        pool.get_or_create_connection(&url, None, connect_ok(conn.clone()))
            .await
            .expect("connects");
        assert!(is_advertised(&pool, &url));

        conn.set(ConnectionLiveness::Retired);
        let entry = pool.get_or_init_pool_entry(&url).await;

        assert!(
            is_advertised(&pool, &url),
            "a rotation the connection chose is not evidence the peer went away"
        );
        assert!(
            entry.connection.get().is_none(),
            "the retired connection must be vacated so the next request re-dials"
        );
    }

    #[tokio::test]
    async fn a_dead_connection_is_dropped_from_the_advertised_set() {
        let (pool, url) = test_pool().await;
        let conn = FakeConn::new();
        pool.get_or_create_connection(&url, None, connect_ok(conn.clone()))
            .await
            .expect("connects");
        assert!(is_advertised(&pool, &url));

        conn.set(ConnectionLiveness::Dead);
        let entry = pool.get_or_init_pool_entry(&url).await;

        assert!(
            !is_advertised(&pool, &url),
            "a peer that actually went away must drop out immediately"
        );
        assert!(entry.connection.get().is_none());
        assert!(
            entry.pre_reconnect_delay() >= Duration::from_millis(500),
            "a dead peer resets to reconnecting, which pays the don't-hammer floor"
        );
    }

    #[tokio::test]
    async fn no_status_tick_is_emitted_across_a_healthy_refresh() {
        let (pool, url) = test_pool().await;
        let conn = FakeConn::new();
        pool.get_or_create_connection(&url, None, connect_ok(conn.clone()))
            .await
            .expect("connects");

        let rx = pool.get_active_connection_receiver();

        conn.set(ConnectionLiveness::Retired);
        pool.get_or_init_pool_entry(&url).await;
        pool.get_or_create_connection(&url, None, connect_ok(FakeConn::new()))
            .await
            .expect("reconnects");

        // Deliberately asserting on the TICK COUNT rather than the final state:
        // tokio `watch` coalesces, so a remove-then-insert leaves the set looking
        // identical while having woken every status consumer twice.
        assert!(
            !rx.has_changed().expect("sender alive"),
            "a healthy refresh must not tick status consumers"
        );
        assert!(is_advertised(&pool, &url));
    }

    #[tokio::test]
    async fn a_refresh_still_tells_consumers_to_re_read_connectivity() {
        // The other half of the contract above. Suppressing the membership tick
        // is what stops the flap, but status consumers also re-read the
        // connector's path on every tick, and a replacement connection can come
        // up relay where its predecessor was direct. The connectors log their
        // initial path without ticking, so without this signal a consumer holds
        // the superseded path indefinitely.
        let (pool, url) = test_pool().await;
        let conn = FakeConn::new();
        pool.get_or_create_connection(&url, None, connect_ok(conn.clone()))
            .await
            .expect("connects");

        let membership_rx = pool.get_active_connection_receiver();
        let path_rx = pool.connectivity_change_notifier();

        conn.set(ConnectionLiveness::Retired);
        pool.get_or_init_pool_entry(&url).await;
        pool.get_or_create_connection(&url, None, connect_ok(FakeConn::new()))
            .await
            .expect("reconnects");

        assert!(
            path_rx.has_changed().expect("sender alive"),
            "a refresh must tell consumers to re-read connectivity"
        );
        assert!(
            !membership_rx.has_changed().expect("sender alive"),
            "and must still not tick membership"
        );
        assert!(is_advertised(&pool, &url));
    }

    #[tokio::test]
    async fn a_failed_reconnect_after_a_refresh_marks_the_peer_disconnected() {
        let (pool, url) = test_pool().await;
        let conn = FakeConn::new();
        pool.get_or_create_connection(&url, None, connect_ok(conn.clone()))
            .await
            .expect("connects");

        conn.set(ConnectionLiveness::Retired);
        pool.get_or_init_pool_entry(&url).await;
        assert!(
            is_advertised(&pool, &url),
            "precondition: the peer stays advertised while the refresh re-dials"
        );

        // `active_connections` is only inserted into after a successful dial, so
        // without the removal on the failure path a peer that died at a refresh
        // boundary would be advertised `Connected` forever.
        pool.get_or_create_connection(&url, None, connect_err())
            .await
            .expect_err("the re-dial fails");

        assert!(
            !is_advertised(&pool, &url),
            "one failed re-dial must un-advertise the peer"
        );
    }

    #[tokio::test]
    async fn the_disconnect_watch_reconciles_a_retirement_without_re_dialing() {
        let connectors = ConnectorRegistry::build_from_testing_defaults()
            .bind()
            .await
            .expect("registry builds");
        let pool = ConnectionPool::new(connectors);
        let url = SafeUrl::parse("ws://guardian.invalid:1234").expect("valid url");
        let conn = AutoRefreshConn::new();
        let attempts = Arc::new(AtomicUsize::new(0));

        pool.get_or_create_connection(&url, None, connect_counted(conn.clone(), attempts.clone()))
            .await
            .expect("initial connection succeeds");
        assert!(is_advertised(&pool, &url));

        // No request follows this retirement. The watcher reconciles the pool
        // entry so the next caller re-dials instead of getting the retired
        // connection back...
        conn.retire();
        fedimint_core::runtime::timeout(Duration::from_secs(1), async {
            loop {
                let vacated = pool
                    .connections
                    .lock()
                    .await
                    .get(&url)
                    .is_some_and(|entry| entry.connection.get().is_none());
                if vacated {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the disconnect watcher reconciles the retirement");

        // ... but it must NOT dial one itself: re-establishing a connection
        // nobody asked for is `Client::spawn_federation_reconnect`'s opt-in job,
        // and a watcher that re-dialed would outlive the client that created the
        // pool, keeping guardian connections alive after logout. The settle
        // window gives such a re-dial every chance to show up in `attempts`;
        // dialing on demand, nothing can move it off 1.
        fedimint_core::runtime::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            1,
            "the pool must dial on demand only"
        );
        assert!(
            is_advertised(&pool, &url),
            "a rotation is not evidence the peer went away, even with no re-dial yet"
        );
    }

    #[tokio::test]
    async fn a_stale_failed_attempt_does_not_evict_a_newer_connection() {
        let (pool, url) = test_pool().await;
        let conn = FakeConn::new();
        pool.get_or_create_connection(&url, None, connect_ok(conn.clone()))
            .await
            .expect("connects");
        let generation_1 = pool
            .connections
            .lock()
            .await
            .get(&url)
            .cloned()
            .expect("entry exists");

        conn.set(ConnectionLiveness::Retired);
        let generation_2 = pool.get_or_init_pool_entry(&url).await;
        assert!(!Arc::ptr_eq(&generation_1, &generation_2));

        // A late failure reported against generation 1 while generation 2 has not
        // connected yet. Only the identity check can turn this away — the
        // already-connected check cannot, because generation 2's cell is still
        // empty — and letting it through would resurrect the very flap this
        // branch exists to kill. Driven through the private helper directly:
        // `get_or_create_connection` cannot produce this interleaving (see
        // `settle_active_flag`), so this pins the helper's contract, not a
        // reachable production race.
        pool.settle_active_flag(&url, &generation_1, false).await;
        assert!(
            is_advertised(&pool, &url),
            "a stale generation's failure must not un-advertise a peer mid-refresh"
        );

        pool.get_or_create_connection(&url, None, connect_ok(FakeConn::new()))
            .await
            .expect("reconnects on the new generation");
        assert!(is_advertised(&pool, &url));

        // Same again once generation 2 is genuinely connected: a late failure
        // from a generation the pool no longer holds says nothing about a peer
        // it is happily talking to.
        pool.settle_active_flag(&url, &generation_1, false).await;
        assert!(
            is_advertised(&pool, &url),
            "a stale generation's failure must not evict a live connection"
        );
    }

    #[tokio::test]
    async fn a_first_connect_failure_does_not_tick_the_active_set() {
        let (pool, url) = test_pool().await;
        let rx = pool.get_active_connection_receiver();

        pool.get_or_create_connection(&url, None, connect_err())
            .await
            .expect_err("the dial fails");

        // The URL was never advertised, so removing it changes nothing — and a
        // no-op must not wake every status consumer.
        assert!(
            !rx.has_changed().expect("sender alive"),
            "a no-op remove must not tick consumers"
        );
    }

    #[test]
    fn new_refreshing_grants_an_immediate_first_redial() {
        let refreshing = ConnectionState::<FakeConn>::new_refreshing();
        assert_eq!(
            refreshing.pre_reconnect_delay(),
            Duration::ZERO,
            "a rotation we chose does not deserve the don't-hammer floor"
        );
        assert!(
            refreshing.pre_reconnect_delay() >= Duration::from_millis(500),
            "but anything past the first attempt looks like a real failure"
        );

        // Contrast: a connection that failed pays the floor up front.
        let reconnecting = ConnectionState::<FakeConn>::new_reconnecting();
        assert!(reconnecting.pre_reconnect_delay() >= Duration::from_millis(500));
    }
}
