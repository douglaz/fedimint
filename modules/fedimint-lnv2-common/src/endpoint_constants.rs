// Federation endpoints
pub const ADD_GATEWAY_ENDPOINT: &str = "add_gateway";

// NOTE: the four payment-wait endpoints below are also listed by name in
// `IROH_LNV2_WAIT_METHODS` in `fedimint-connectors/src/iroh.rs`, which gives
// them a shorter iroh request budget than other long-polls. That crate cannot
// depend on this one, so the names are duplicated there as literals, and
// renaming one here without updating there silently changes its budget — no
// test catches it. The three `await_*` names would still match that file's
// `await_`/`wait_` prefix heuristic and fall back to the 1-hour tier;
// `decryption_key_share` matches neither and would fall all the way back to the
// 60s prompt tier, which for a wait the gateway issues before funding
// acceptance means retiring the shared pooled connection once a minute for the
// length of the wait. Retirement leaves the requests already in flight on that
// connection alone, but it does force every new request onto a freshly dialed
// one.
pub const AWAIT_INCOMING_CONTRACT_ENDPOINT: &str = "await_incoming_contract";
pub const AWAIT_PREIMAGE_ENDPOINT: &str = "await_preimage";
pub const AWAIT_INCOMING_CONTRACTS_ENDPOINT: &str = "await_incoming_contracts";
pub const DECRYPTION_KEY_SHARE_ENDPOINT: &str = "decryption_key_share";
pub const CONSENSUS_BLOCK_COUNT_ENDPOINT: &str = "consensus_block_count";
pub const GATEWAYS_ENDPOINT: &str = "gateways";
pub const OUTGOING_CONTRACT_EXPIRATION_ENDPOINT: &str = "outgoing_contract_expiration";
pub const REMOVE_GATEWAY_ENDPOINT: &str = "remove_gateway";

// Gateway endpoints
pub const CREATE_BOLT11_INVOICE_ENDPOINT: &str = "/create_bolt11_invoice";
pub const VERIFY_BOLT11_PREIMAGE_ENDPOINT: &str = "/verify_bolt11_preimage";
pub const ROUTING_INFO_ENDPOINT: &str = "/routing_info";
pub const SEND_PAYMENT_ENDPOINT: &str = "/send_payment";
