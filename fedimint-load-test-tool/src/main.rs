use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use bitcoin::secp256k1;
use clap::{Parser, Subcommand, ValueEnum};
use devimint::cmd;
use devimint::util::ToCmdExt;
use fedimint_client::secret::PlainRootSecretStrategy;
use fedimint_client::sm::OperationId;
use fedimint_client::transaction::TransactionBuilder;
use fedimint_client::{Client, ClientBuilder};
use fedimint_core::api::{
    GlobalFederationApi, IFederationApi, WsClientConnectInfo, WsFederationApi,
};
use fedimint_core::config::ClientConfig;
use fedimint_core::core::IntoDynInstance;
use fedimint_core::encoding::Decodable;
use fedimint_core::module::registry::ModuleDecoderRegistry;
use fedimint_core::module::CommonModuleGen;
use fedimint_core::task::TaskGroup;
use fedimint_core::util::BoxFuture;
use fedimint_core::{Amount, TieredMulti, TieredSummary};
use fedimint_ln_client::{LightningClientExt, LightningClientGen, LnPayState};
use fedimint_mint_client::{
    MintClientExt, MintClientGen, MintClientModule, MintCommonGen, MintMeta, SpendableNote,
};
use fedimint_wallet_client::WalletClientGen;
use futures::StreamExt;
use lightning_invoice::Invoice;
use tokio::sync::mpsc;
use tracing::{info, warn};

#[derive(Parser, Clone)]
#[command(version)]
struct Opts {
    #[arg(long, default_value = "1000")]
    users: usize,

    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum LnType {
    Cln,
    Lnd,
}

#[derive(Subcommand, Clone)]
enum Command {
    TestDownload {
        connect: String,
    },
    RunEconomy {
        #[arg(long)]
        connect: Option<String>,

        #[clap(value_parser = parse_ecash)]
        #[arg(long)]
        initial_notes: Option<TieredMulti<SpendableNote>>,

        #[arg(long)]
        generate_invoice_with: LnType,

        #[arg(long)]
        gateway_public_key: Option<String>,
    },
}

#[derive(Debug, Clone)]
struct Event(String, Duration);

// #[tokio::main(worker_threads = 10000)]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    fedimint_logging::TracingSetup::default().init()?;
    let opts = Opts::parse();
    let (event_sender, mut event_receiver) = tokio::sync::mpsc::unbounded_channel();
    let futures = match opts.command {
        Command::TestDownload { connect } => {
            test_download_config(&connect, opts.users, event_sender.clone()).await?
        }
        Command::RunEconomy {
            connect,
            initial_notes,
            generate_invoice_with,
            gateway_public_key,
        } => {
            let connect = if let Some(connect) = connect {
                connect.to_owned()
            } else {
                cmd!("fedimint-cli", "connect-info").out_json().await?["connect_info"]
                    .as_str()
                    .map(ToOwned::to_owned)
                    .expect("connect-info command to succeed")
            };
            let initial_notes = if let Some(initial_notes) = initial_notes {
                initial_notes.to_owned()
            } else {
                cmd!("fedimint-cli", "spend", "1000sats").out_json().await?["note"]
                    .as_str()
                    .map(parse_ecash)
                    .transpose()?
                    .expect("spend command to succeed")
            };

            let gateway_public_key = if let Some(gateway_public_key) = gateway_public_key {
                gateway_public_key
            } else {
                get_gateway_public_key(generate_invoice_with).await?
            };
            run_economy(
                opts.users,
                connect,
                initial_notes,
                generate_invoice_with,
                gateway_public_key,
                event_sender.clone(),
            )
            .await?
        }
    };
    let summary_handle = tokio::spawn(async move {
        let mut results = HashMap::new();
        while let Some(event) = event_receiver.recv().await {
            let entry = results.entry(event.0).or_insert_with(Vec::new);
            entry.push(event.1);
        }
        for (k, mut v) in results {
            v.sort();
            let len = v.len();
            let max = v.iter().last().unwrap();
            let min = v.first().unwrap();
            let median = v[len / 2];
            let sum: Duration = v.iter().sum();
            let avg = sum / len as u32;
            eprintln!("{len} {k}: avg {avg:?}, median {median:?}, max {max:?}, min {min:?}");
        }
    });
    let result = futures::future::join_all(futures).await;
    drop(event_sender);
    summary_handle.await?;
    eprintln!(
        "{} results, {} failures",
        result.len(),
        result.iter().filter(|r| r.is_err()).count()
    );
    for r in result {
        if let Err(e) = r {
            warn!("Task failed: {:?}", e);
        }
    }
    Ok(())
}

async fn run_economy(
    users: usize,
    connect: String,
    initial_notes: TieredMulti<SpendableNote>,
    ln_invoice: LnType,
    gateway_public_key: String,
    event_sender: mpsc::UnboundedSender<Event>,
) -> anyhow::Result<Vec<BoxFuture<'static, anyhow::Result<()>>>> {
    let connect_obj: WsClientConnectInfo =
        WsClientConnectInfo::from_str(&connect).context("invalid connect info")?;
    let api = Arc::new(WsFederationApi::from_connect_info(&[connect_obj.clone()]))
        as Arc<dyn IFederationApi + Send + Sync + 'static>;
    let cfg: ClientConfig = api.download_client_config(&connect_obj).await?;

    let mut tg = TaskGroup::new();

    let coordinator = build_client(&cfg, &mut tg).await?;
    let mut users_clients = Vec::with_capacity(users);
    for _ in 0..users {
        let client = build_client(&cfg, &mut tg).await?;
        switch_default_gateway(&client, &gateway_public_key).await?;
        users_clients.push(client);
    }

    // distribute notes
    let mut amount = initial_notes.total_amount();
    let per_user = Amount::from_msats(1100); //amount.msats / (users as u64) / 10);

    info!("Reissuing initial notes: initial {amount}, per user: {per_user}");
    reissue_notes(&coordinator, initial_notes, &event_sender).await?;

    generate_outputs(&coordinator, Amount::from_msats(1024), 29).await?;

    let summary = get_note_summary(&coordinator).await?;
    for (k, v) in summary.iter() {
        info!("{k}: {v}");
    }
    anyhow::bail!("finished");

    let mut notes_per_user = Vec::new();
    for u in 0..users {
        let (_, notes) = do_spend_notes(&coordinator, per_user, &event_sender).await?;
        let user_amount = notes.total_amount();
        info!("Giving {user_amount} to user {u}");
        notes_per_user.push(notes);
        amount -= user_amount;
        // // Reissue to rebalance notes
        // let (_, notes) = do_spend_notes(&coordinator, amount.clone(),
        // &event_sender).await?; info!(
        //     "Our amount is {amount}, notes total amount is {}",
        //     notes.total_amount()
        // );
        // reissue_notes(&coordinator, notes, &event_sender).await?;
    }

    info!("Starting user tasks");
    let futures = users_clients
        .into_iter()
        .zip(notes_per_user)
        .map(|(client, notes)| {
            let event_sender = event_sender.clone();
            let f: BoxFuture<_> = Box::pin(do_user_task(client, notes, ln_invoice, event_sender));
            f
        })
        .collect::<Vec<_>>();

    Ok(futures)
}

enum UserTask {
    UseNotes(TieredMulti<SpendableNote>),
}

async fn do_user_task(
    client: Client,
    notes: TieredMulti<SpendableNote>,
    // mut tasks: mpsc::Receiver<UserTask>,
    // partner_sender: mpsc::Sender<UserTask>,
    ln_invoice: LnType,
    event_sender: mpsc::UnboundedSender<Event>,
) -> anyhow::Result<()> {
    // while let Some(task) = tasks.recv().await {
    //     match task {
    //         UserTask::UseNotes(notes) => {
    //             let amount = notes.total_amount();
    //             reissue_notes(&client, notes, &event_sender)
    //                 .await
    //                 .map_err(|e| anyhow::anyhow!("while reissuing initial
    // {amount}: {e}"))?;             let notes = do_spend_notes(&client,
    // amount, &event_sender)                 .await
    //                 .map_err(|e| anyhow::anyhow!("while spending {amount} on
    // iteration: {e}"))?;

    //         }
    //     }
    // }
    let amount = notes.total_amount();
    reissue_notes(&client, notes, &event_sender)
        .await
        .map_err(|e| anyhow::anyhow!("while reissuing initial {amount}: {e}"))?;
    let invoice_amount = Amount::from_msats(1000);
    if invoice_amount > amount {
        warn!("Can't pay invoice, not enough funds: {invoice_amount} > {amount}");
    } else {
        match ln_invoice {
            LnType::Cln => {
                let (invoice, label) = cln_create_invoice(invoice_amount).await?;
                gateway_pay_invoice(&client, invoice, &event_sender).await?;
                cln_wait_invoice_payment(&label).await?;
            }
            LnType::Lnd => {
                let (invoice, r_hash) = lnd_create_invoice(invoice_amount).await?;
                gateway_pay_invoice(&client, invoice, &event_sender).await?;
                lnd_wait_invoice_payment(r_hash).await?;
            }
        };
    }

    Ok(())
}

async fn reissue_notes(
    client: &Client,
    notes: TieredMulti<SpendableNote>,
    event_sender: &mpsc::UnboundedSender<Event>,
) -> anyhow::Result<()> {
    let m = fedimint_core::time::now();
    let operation_id = client.reissue_external_notes(notes, ()).await?;
    let mut updates = client
        .subscribe_reissue_external_notes_updates(operation_id)
        .await
        .unwrap();
    while let Some(update) = updates.next().await {
        if let fedimint_mint_client::ReissueExternalNotesState::Failed(e) = update {
            return Err(anyhow::Error::msg(format!("Reissue failed: {e}")));
        }
    }
    event_sender.send(Event("reissue_notes".into(), m.elapsed().unwrap()))?;
    Ok(())
}

async fn do_spend_notes(
    client: &Client,
    amount: Amount,
    event_sender: &mpsc::UnboundedSender<Event>,
) -> anyhow::Result<(OperationId, TieredMulti<SpendableNote>)> {
    let m = fedimint_core::time::now();
    let (operation_id, notes) = client
        .spend_notes(amount, Duration::from_secs(600), ())
        .await?;
    let mut updates = client
        .subscribe_spend_notes_updates(operation_id)
        .await
        .unwrap();
    while let Some(update) = updates.next().await {
        match update {
            fedimint_mint_client::SpendOOBState::Created
            | fedimint_mint_client::SpendOOBState::Success => break, // early break
            other => {
                return Err(anyhow::Error::msg(format!("Spend failed: {other:?}")));
            }
        }
    }
    event_sender.send(Event("spend_notes".into(), m.elapsed().unwrap()))?;
    Ok((operation_id, notes))
}

async fn await_spend_notes_finish(
    client: &Client,
    operation_id: OperationId,
) -> anyhow::Result<()> {
    let mut updates = client
        .subscribe_spend_notes_updates(operation_id)
        .await
        .unwrap();
    while let Some(update) = updates.next().await {
        info!("SpendOOBState update: {:?}", update);
        match update {
            fedimint_mint_client::SpendOOBState::Created
            | fedimint_mint_client::SpendOOBState::Success => {}
            other => {
                return Err(anyhow::Error::msg(format!("Spend failed: {other:?}")));
            }
        }
    }
    Ok(())
}

async fn test_download_config(
    connect: &str,
    users: usize,
    event_sender: mpsc::UnboundedSender<Event>,
) -> anyhow::Result<Vec<BoxFuture<'static, anyhow::Result<()>>>> {
    let connect_obj: WsClientConnectInfo =
        WsClientConnectInfo::from_str(connect).context("invalid connect info")?;
    let api = Arc::new(WsFederationApi::from_connect_info(&[connect_obj.clone()]))
        as Arc<dyn IFederationApi + Send + Sync + 'static>;

    Ok((0..users)
        .map(|_| {
            let api = api.clone();
            let connect_obj = connect_obj.clone();
            let event_sender = event_sender.clone();
            let f: BoxFuture<_> = Box::pin(async move {
                let m = fedimint_core::time::now();
                let _ = api.download_client_config(&connect_obj).await?;
                event_sender.send(Event("download_client_config".into(), m.elapsed().unwrap()))?;
                Ok(())
            });
            f
        })
        .collect())
}

async fn build_client(cfg: &ClientConfig, tg: &mut TaskGroup) -> anyhow::Result<Client> {
    let mut client_builder = ClientBuilder::default();
    client_builder.with_module(MintClientGen);
    client_builder.with_module(LightningClientGen);
    client_builder.with_module(WalletClientGen);
    client_builder.with_primary_module(1);
    client_builder.with_config(cfg.clone());
    let db = fedimint_core::db::mem_impl::MemDatabase::new();
    client_builder.with_database(db);
    let client = client_builder.build::<PlainRootSecretStrategy>(tg).await?;
    Ok(client)
}

pub fn parse_ecash(s: &str) -> anyhow::Result<TieredMulti<SpendableNote>> {
    let bytes = base64::decode(s)?;
    Ok(Decodable::consensus_decode(
        &mut std::io::Cursor::new(bytes),
        &ModuleDecoderRegistry::default(),
    )?)
}

async fn lnd_create_invoice(amount: Amount) -> anyhow::Result<(Invoice, String)> {
    let result = cmd!(LnCli, "addinvoice", "--amt_msat", amount.msats)
        .out_json()
        .await?;
    let invoice = result["payment_request"]
        .as_str()
        .map(Invoice::from_str)
        .transpose()?
        .ok_or_else(|| anyhow::anyhow!("Missing payment_request field"))?;
    let r_hash = result["r_hash"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("Missing r_hash field"))?
        .to_owned();
    Ok((invoice, r_hash))
}

async fn lnd_wait_invoice_payment(r_hash: String) -> anyhow::Result<()> {
    for _ in 0..60 {
        let result = cmd!(LnCli, "lookupinvoice", &r_hash).out_json().await?;
        let state = result["state"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Missing state field"))?;
        if state == "SETTLED" {
            return Ok(());
        } else {
            fedimint_core::task::sleep(Duration::from_millis(500)).await;
        }
    }
    anyhow::bail!("Timeout waiting for invoice to settle: {r_hash}")
}

async fn gateway_pay_invoice(
    client: &Client,
    invoice: Invoice,
    event_sender: &mpsc::UnboundedSender<Event>,
) -> anyhow::Result<()> {
    let m = fedimint_core::time::now();
    let (operation_id, _transaction_id) = client
        .pay_bolt11_invoice(client.federation_id(), invoice)
        .await?;

    let mut updates = client.subscribe_ln_pay_updates(operation_id).await?;

    while let Some(update) = updates.next().await {
        match update {
            LnPayState::Success { preimage: _ } => {
                break;
            }
            LnPayState::Created | LnPayState::Funded => {}
            LnPayState::Refunded { refund_txid } => {
                client.await_claim_notes(operation_id, refund_txid).await?;
                bail!("Failed to pay invoice: Refunded")
            }
            other @ (LnPayState::Canceled
            | LnPayState::Failed
            | LnPayState::WaitingForRefund { .. }) => {
                bail!("Failed to pay invoice: {other:?}")
            }
        }
        info!("LnPayState update: {:?}", update);
    }
    event_sender.send(Event("gateway_pay_invoice".into(), m.elapsed().unwrap()))?;
    Ok(())
}

async fn cln_create_invoice(amount: Amount) -> anyhow::Result<(Invoice, String)> {
    let now = fedimint_core::time::now();
    let random_n: u128 = rand::random();
    let label = format!("label-{now:?}-{random_n}");
    let invoice_string = cmd!(ClnLightningCli, "invoice", amount.msats, &label, &label)
        .out_json()
        .await?["bolt11"]
        .as_str()
        .ok_or_else(|| anyhow!("Missing bolt11 field"))?
        .to_owned();
    Ok((Invoice::from_str(&invoice_string)?, label))
}

async fn cln_wait_invoice_payment(label: &str) -> anyhow::Result<()> {
    let status = cmd!(ClnLightningCli, "waitinvoice", label)
        .out_json()
        .await?["status"]
        .as_str()
        .ok_or_else(|| anyhow!("Missing status field"))?
        .to_owned();
    if status == "paid" {
        Ok(())
    } else {
        bail!("Got status {status} for invoice {label}")
    }
}

async fn switch_default_gateway(client: &Client, gateway_public_key: &str) -> anyhow::Result<()> {
    let gateway_public_key = parse_node_pub_key(gateway_public_key)?;
    client.set_active_gateway(&gateway_public_key).await?;
    Ok(())
}

// async fn switch_to_cln_gateway(client: &Client) -> anyhow::Result<()> {
//     let node_pub_key = cmd!("gateway-cln", "info", "node_pub_key")
//         .out_json()
//         .await?["lightning_pub_key"]
//         .as_str()
//         .ok_or_else(|| anyhow!("Missing lightning_pub_key field"))?;
//     client.switch_active_gateway(node_pub_key, dbtx);
//     Ok(())
// }

async fn get_gateway_public_key(generate_invoice_with: LnType) -> anyhow::Result<String> {
    let gateway_json = match generate_invoice_with {
        LnType::Cln => {
            // If we are paying a lnd invoice, we use the cln gateway
            cmd!(GatewayLndCli, "info").out_json().await
        }
        LnType::Lnd => {
            // and vice-versa
            cmd!(GatewayClnCli, "info").out_json().await
        }
    }?;
    let node_pub_key = gateway_json["federations"][0]["registration"]["node_pub_key"]
        .as_str()
        .ok_or_else(|| anyhow!("Missing node_pub_key field"))?;

    Ok(node_pub_key.into())
}

fn parse_node_pub_key(s: &str) -> Result<secp256k1::PublicKey, secp256k1::Error> {
    secp256k1::PublicKey::from_str(s)
}

struct LnCli;
impl ToCmdExt for LnCli {
    type Fut = std::future::Ready<devimint::util::Command>;

    fn cmd(self) -> Self::Fut {
        // try to use alias if set
        let lncli = std::env::var("FM_LNCLI")
            .map(|s| s.split_whitespace().map(ToOwned::to_owned).collect())
            .unwrap_or_else(|_| vec!["lncli".into()]);
        let mut cmd = tokio::process::Command::new(&lncli[0]);
        cmd.args(&lncli[1..]);
        std::future::ready(devimint::util::Command {
            cmd,
            args_debug: lncli,
        })
    }
}

async fn get_note_summary(client: &Client) -> anyhow::Result<TieredSummary> {
    let (mint_client, _) = client.get_first_module::<MintClientModule>(&fedimint_mint_client::KIND);
    let summary = mint_client
        .get_wallet_summary(&mut client.db().begin_transaction().await.with_module_prefix(1))
        .await;
    Ok(summary)
}

async fn generate_outputs(
    client: &Client,
    output_amount: Amount,
    quantity: u16,
) -> anyhow::Result<()> {
    let input_amount = output_amount * quantity as u64 * 2;
    let (mint_client, client_module_instance) =
        client.get_first_module::<MintClientModule>(&fedimint_mint_client::KIND);

    let mut dbtx = client.db().begin_transaction().await;
    let operation_id = OperationId::new_random();
    let mut module_transaction = dbtx.with_module_prefix(client_module_instance.id);

    let mint_input = mint_client
        .create_input(&mut module_transaction, operation_id, input_amount)
        .await?;
    let mut tx =
        TransactionBuilder::new().with_input(mint_input.into_dyn(client_module_instance.id));

    for _ in 0..quantity {
        let output = mint_client
            .create_output(&mut module_transaction, operation_id, 1, output_amount)
            .await;
        tx = tx.with_output(output.into_dyn(client_module_instance.id));
    }
    drop(module_transaction);

    let operation_meta_gen = |_txid| ();

    let txid = client
        .finalize_and_submit_transaction(
            operation_id,
            MintCommonGen::KIND.as_str(),
            operation_meta_gen,
            tx,
        )
        .await
        .expect("Transactions can only fail if the operation already exists");
    let tx_subscription = client.transaction_updates(operation_id).await;
    tx_subscription.await_tx_accepted(txid).await?;
    dbtx.commit_tx().await;
    for i in 0..quantity {
        let out_point = fedimint_core::OutPoint {
            txid,
            out_idx: i as u64,
        };
        mint_client
            .await_output_finalized(operation_id, out_point)
            .await?;
    }
    Ok(())
}

struct ClnLightningCli;
impl ToCmdExt for ClnLightningCli {
    type Fut = std::future::Ready<devimint::util::Command>;

    fn cmd(self) -> Self::Fut {
        // try to use alias if set
        let lightning_cli = std::env::var("FM_LIGHTNING_CLI")
            .map(|s| s.split_whitespace().map(ToOwned::to_owned).collect())
            .unwrap_or_else(|_| vec!["lightning-cli".into()]);
        let mut cmd = tokio::process::Command::new(&lightning_cli[0]);
        cmd.args(&lightning_cli[1..]);
        std::future::ready(devimint::util::Command {
            cmd,
            args_debug: lightning_cli,
        })
    }
}

struct GatewayClnCli;
impl ToCmdExt for GatewayClnCli {
    type Fut = std::future::Ready<devimint::util::Command>;

    fn cmd(self) -> Self::Fut {
        // try to use alias if set
        let gw = std::env::var("FM_GWCLI_CLN")
            .map(|s| s.split_whitespace().map(ToOwned::to_owned).collect())
            .unwrap_or_else(|_| vec!["gateway-cln".into()]);
        let mut cmd = tokio::process::Command::new(&gw[0]);
        cmd.args(&gw[1..]);
        std::future::ready(devimint::util::Command {
            cmd,
            args_debug: gw,
        })
    }
}

struct GatewayLndCli;
impl ToCmdExt for GatewayLndCli {
    type Fut = std::future::Ready<devimint::util::Command>;

    fn cmd(self) -> Self::Fut {
        // try to use alias if set
        let gw = std::env::var("FM_GWCLI_LND")
            .map(|s| s.split_whitespace().map(ToOwned::to_owned).collect())
            .unwrap_or_else(|_| vec!["gateway-lnd".into()]);
        let mut cmd = tokio::process::Command::new(&gw[0]);
        cmd.args(&gw[1..]);
        std::future::ready(devimint::util::Command {
            cmd,
            args_debug: gw,
        })
    }
}
