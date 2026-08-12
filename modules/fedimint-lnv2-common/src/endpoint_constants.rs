// Federation endpoints
pub const ADD_GATEWAY_ENDPOINT: &str = "add_gateway";

// NOTE: the four payment-wait endpoints below are also listed by name in
// `IROH_LNV2_WAIT_METHODS` in `fedimint-connectors/src/iroh.rs`, which gives
// them a shorter iroh request budget than other long-polls. That crate cannot
// depend on this one, so the names are duplicated there as literals, and
// renaming one here without updating there would silently change its budget.
// `the_payment_waits_keep_their_own_iroh_budget` at the bottom of this file is
// the gate for that — this crate CAN see both sides.
//
// What it protects against, if you are tempted to drop it: the three `await_*`
// names would still match that file's `await_`/`wait_` prefix heuristic and
// fall back to the 1-hour tier; `decryption_key_share` matches neither and
// would fall all the way back to the 60s prompt tier, which for a wait the
// gateway issues before funding acceptance means retiring the shared pooled
// connection once a minute for the length of the wait. Retirement leaves the
// requests already in flight on that connection alone, but it does force every
// new request onto a freshly dialed one.
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use fedimint_connectors::iroh::request_timeout_for_method;
    use fedimint_core::module::ApiMethod;

    use super::{
        AWAIT_INCOMING_CONTRACT_ENDPOINT, AWAIT_INCOMING_CONTRACTS_ENDPOINT,
        AWAIT_PREIMAGE_ENDPOINT, DECRYPTION_KEY_SHARE_ENDPOINT,
    };

    fn budget(name: &str) -> Duration {
        request_timeout_for_method(&ApiMethod::Module(0, name.to_owned()))
    }

    /// The four payment waits are given their iroh budget by name, from a list
    /// of literals in `fedimint-connectors` that cannot reference these
    /// constants. This is the gate for that: rename one here without updating
    /// the list there and this fails.
    ///
    /// Asserted as an ordering against a method from each neighbouring tier
    /// rather than against the tier constant itself, so the bound survives the
    /// values being retuned. Both directions are load-bearing and they catch
    /// different renames: the `await_*` names would still match the prefix
    /// heuristic and silently fall UP to the 1-hour tier, while
    /// `decryption_key_share` matches no prefix and would fall all the way DOWN
    /// to the prompt tier.
    #[test]
    fn the_payment_waits_keep_their_own_iroh_budget() {
        let long_poll = budget("await_transaction");
        let prompt = budget("block_count");
        assert!(prompt < long_poll, "tiers are ordered as this test assumes");

        for endpoint in [
            AWAIT_INCOMING_CONTRACT_ENDPOINT,
            AWAIT_INCOMING_CONTRACTS_ENDPOINT,
            AWAIT_PREIMAGE_ENDPOINT,
            DECRYPTION_KEY_SHARE_ENDPOINT,
        ] {
            let budget = budget(endpoint);
            assert!(
                budget < long_poll,
                "{endpoint} fell back to the long-poll tier - is it still named in \
                 IROH_LNV2_WAIT_METHODS?"
            );
            assert!(
                budget > prompt,
                "{endpoint} fell back to the prompt tier - is it still named in \
                 IROH_LNV2_WAIT_METHODS?"
            );
        }
    }
}
