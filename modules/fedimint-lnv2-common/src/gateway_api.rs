use std::ops::Add;
use std::str::FromStr;

use bitcoin::secp256k1::PublicKey;
use bitcoin::secp256k1::schnorr::Signature;
use fedimint_connectors::error::ServerError;
use fedimint_core::config::FederationId;
use fedimint_core::encoding::{Decodable, Encodable};
use fedimint_core::util::SafeUrl;
use fedimint_core::{Amount, OutPoint, apply, async_trait_maybe_send};
use fedimint_ln_common::client::GatewayApi;
use lightning_invoice::{Bolt11Invoice, RoutingFees};
use reqwest::Method;
use serde::{Deserialize, Serialize};

use crate::contracts::{IncomingContract, OutgoingContract};
use crate::endpoint_constants::{
    CREATE_BOLT11_INVOICE_ENDPOINT, ROUTING_INFO_ENDPOINT, SEND_PAYMENT_ENDPOINT,
};
use crate::{Bolt11InvoiceDescription, LightningInvoice};

#[apply(async_trait_maybe_send!)]
pub trait GatewayConnection: std::fmt::Debug {
    async fn routing_info(
        &self,
        gateway_api: SafeUrl,
        federation_id: &FederationId,
    ) -> Result<Option<RoutingInfo>, ServerError>;

    async fn bolt11_invoice(
        &self,
        gateway_api: SafeUrl,
        federation_id: FederationId,
        contract: IncomingContract,
        amount: Amount,
        description: Bolt11InvoiceDescription,
        expiry_secs: u32,
    ) -> Result<Bolt11Invoice, ServerError>;

    async fn send_payment(
        &self,
        gateway_api: SafeUrl,
        federation_id: FederationId,
        outpoint: OutPoint,
        contract: OutgoingContract,
        invoice: LightningInvoice,
        auth: Signature,
    ) -> Result<Result<[u8; 32], Signature>, ServerError>;
}

#[derive(Debug, Clone)]
pub struct RealGatewayConnection {
    pub api: GatewayApi,
}

#[apply(async_trait_maybe_send!)]
impl GatewayConnection for RealGatewayConnection {
    async fn routing_info(
        &self,
        gateway_api: SafeUrl,
        federation_id: &FederationId,
    ) -> Result<Option<RoutingInfo>, ServerError> {
        self.api
            .request(
                &gateway_api,
                Method::POST,
                ROUTING_INFO_ENDPOINT,
                Some(federation_id),
            )
            .await
    }

    async fn bolt11_invoice(
        &self,
        gateway_api: SafeUrl,
        federation_id: FederationId,
        contract: IncomingContract,
        amount: Amount,
        description: Bolt11InvoiceDescription,
        expiry_secs: u32,
    ) -> Result<Bolt11Invoice, ServerError> {
        self.api
            .request(
                &gateway_api,
                Method::POST,
                CREATE_BOLT11_INVOICE_ENDPOINT,
                Some(CreateBolt11InvoicePayload {
                    federation_id,
                    contract,
                    amount,
                    description,
                    expiry_secs,
                }),
            )
            .await
    }

    async fn send_payment(
        &self,
        gateway_api: SafeUrl,
        federation_id: FederationId,
        outpoint: OutPoint,
        contract: OutgoingContract,
        invoice: LightningInvoice,
        auth: Signature,
    ) -> Result<Result<[u8; 32], Signature>, ServerError> {
        self.api
            .request(
                &gateway_api,
                Method::POST,
                SEND_PAYMENT_ENDPOINT,
                Some(SendPaymentPayload {
                    federation_id,
                    outpoint,
                    contract,
                    invoice,
                    auth,
                }),
            )
            .await
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct CreateBolt11InvoicePayload {
    pub federation_id: FederationId,
    pub contract: IncomingContract,
    pub amount: Amount,
    pub description: Bolt11InvoiceDescription,
    pub expiry_secs: u32,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct SendPaymentPayload {
    pub federation_id: FederationId,
    pub outpoint: OutPoint,
    pub contract: OutgoingContract,
    pub invoice: LightningInvoice,
    pub auth: Signature,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct RoutingInfo {
    /// The public key of the gateways lightning node. Since this key signs the
    /// gateways invoices the senders client uses it to differentiate between a
    /// direct swap between fedimints and a lightning swap.
    pub lightning_public_key: PublicKey,
    /// The human-readable alias of the gateway's lightning node, if available.
    ///
    /// This field is optional for backwards-compatibility with older gateways
    /// that do not yet provide an alias in their `routing_info` responses.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lightning_alias: Option<String>,
    /// The public key of the gateways client module. This key is used to claim
    /// or cancel outgoing contracts and refund incoming contracts.
    pub module_public_key: PublicKey,
    /// This is the fee the gateway charges for an outgoing payment. The senders
    /// client will use this fee in case of a direct swap.
    pub send_fee_minimum: PaymentFee,
    /// This is the default total fee the gateway recommends for an outgoing
    /// payment in case of a lightning swap. It accounts for the additional fee
    /// required to reliably route this payment over lightning.
    pub send_fee_default: PaymentFee,
    /// This is the minimum expiration delta in block the gateway requires for
    /// an outgoing payment. The senders client will use this expiration delta
    /// in case of a direct swap.
    pub expiration_delta_minimum: u64,
    /// This is the default total expiration the gateway recommends for an
    /// outgoing payment in case of a lightning swap. It accounts for the
    /// additional expiration delta required to successfully route this payment
    /// over lightning.
    pub expiration_delta_default: u64,
    /// This is the fee the gateway charges for an incoming payment.
    pub receive_fee: PaymentFee,
}

impl RoutingInfo {
    pub fn send_parameters(&self, invoice: &Bolt11Invoice) -> (PaymentFee, u64) {
        if invoice.recover_payee_pub_key() == self.lightning_public_key {
            (self.send_fee_minimum, self.expiration_delta_minimum)
        } else {
            (self.send_fee_default, self.expiration_delta_default)
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize, Encodable, Decodable, Copy)]
pub struct PaymentFee {
    pub base: Amount,
    pub parts_per_million: u64,
}

impl PaymentFee {
    /// This is the maximum send fee of one and a half percent plus one hundred
    /// satoshis a correct gateway may recommend as a default. It accounts for
    /// the fee required to reliably route this payment over lightning.
    pub const SEND_FEE_LIMIT: PaymentFee = PaymentFee {
        base: Amount::from_sats(100),
        parts_per_million: 15_000,
    };

    /// This is the fee the gateway uses to cover transaction fees with the
    /// federation.
    pub const TRANSACTION_FEE_DEFAULT: PaymentFee = PaymentFee {
        base: Amount::from_sats(2),
        parts_per_million: 3000,
    };

    /// This is the maximum receive fee of half of one percent plus fifty
    /// satoshis a correct gateway may recommend as a default.
    pub const RECEIVE_FEE_LIMIT: PaymentFee = PaymentFee {
        base: Amount::from_sats(50),
        parts_per_million: 5_000,
    };

    /// Returns whether this fee is no greater than `limit` for EVERY payment
    /// amount.
    ///
    /// Both components must be bounded, because which of two fees costs more
    /// depends on the amount being paid: a large base with no proportional
    /// part is cheaper than a purely proportional rate on a big payment and
    /// dearer on a small one. Comparing the structs themselves cannot express
    /// that, so limits are checked componentwise instead.
    pub fn is_within(&self, limit: &PaymentFee) -> bool {
        self.base <= limit.base && self.parts_per_million <= limit.parts_per_million
    }

    pub fn add_to(&self, msats: u64) -> Amount {
        Amount::from_msats(msats.saturating_add(self.absolute_fee(msats)))
    }

    pub fn subtract_from(&self, msats: u64) -> Amount {
        Amount::from_msats(msats.saturating_sub(self.absolute_fee(msats)))
    }

    pub fn fee(&self, msats: u64) -> Amount {
        Amount::from_msats(self.absolute_fee(msats))
    }

    fn absolute_fee(&self, msats: u64) -> u64 {
        msats
            .saturating_mul(self.parts_per_million)
            .saturating_div(1_000_000)
            .saturating_add(self.base.msats)
    }
}

impl Add for PaymentFee {
    type Output = PaymentFee;
    fn add(self, rhs: Self) -> Self::Output {
        // Saturate rather than wrap, since a sum is checked against a limit before
        // it is charged: an overflowing component must not come out the other side
        // as a small one that is within the limit.
        PaymentFee {
            base: Amount::from_msats(self.base.msats.saturating_add(rhs.base.msats)),
            parts_per_million: self.parts_per_million.saturating_add(rhs.parts_per_million),
        }
    }
}

impl From<RoutingFees> for PaymentFee {
    fn from(value: RoutingFees) -> Self {
        PaymentFee {
            base: Amount::from_msats(u64::from(value.base_msat)),
            parts_per_million: u64::from(value.proportional_millionths),
        }
    }
}

impl From<PaymentFee> for RoutingFees {
    fn from(value: PaymentFee) -> Self {
        RoutingFees {
            base_msat: u32::try_from(value.base.msats).expect("base msat was truncated from u64"),
            proportional_millionths: u32::try_from(value.parts_per_million)
                .expect("ppm was truncated from u64"),
        }
    }
}

impl std::fmt::Display for PaymentFee {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{},{}", self.base, self.parts_per_million)
    }
}

impl FromStr for PaymentFee {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.split(',');
        let base_str = parts
            .next()
            .ok_or(anyhow::anyhow!("Failed to parse base fee"))?;
        let ppm_str = parts.next().ok_or(anyhow::anyhow!("Failed to parse ppm"))?;

        // Ensure no extra parts
        if parts.next().is_some() {
            return Err(anyhow::anyhow!(
                "Failed to parse fees. Expected format <base>,<ppm>"
            ));
        }

        let base = Amount::from_str(base_str)?;
        let parts_per_million = ppm_str.parse::<u64>()?;

        Ok(PaymentFee {
            base,
            parts_per_million,
        })
    }
}

#[cfg(test)]
mod tests {
    use fedimint_core::Amount;

    use super::PaymentFee;

    /// A purely proportional rate of one hundred percent. Ordering `PaymentFee`
    /// as a struct compares `base` first, so this rate used to pass a limit
    /// check against `SEND_FEE_LIMIT` while charging the entire payment.
    const CONFISCATORY_RATE: PaymentFee = PaymentFee {
        base: Amount::ZERO,
        parts_per_million: 1_000_000,
    };

    #[test]
    fn confiscatory_rate_is_not_within_send_fee_limit() {
        assert!(!CONFISCATORY_RATE.is_within(&PaymentFee::SEND_FEE_LIMIT));

        // The cost that motivates the check: on a ten thousand satoshi payment the
        // rate charges the whole payment where the limit intends 250 satoshis.
        assert_eq!(CONFISCATORY_RATE.fee(10_000_000), Amount::from_sats(10_000));
        assert_eq!(
            PaymentFee::SEND_FEE_LIMIT.fee(10_000_000),
            Amount::from_sats(250)
        );
    }

    #[test]
    fn a_fee_exceeding_either_component_is_not_within_the_limit() {
        let excessive_base = PaymentFee {
            base: PaymentFee::SEND_FEE_LIMIT.base + Amount::from_msats(1),
            parts_per_million: PaymentFee::SEND_FEE_LIMIT.parts_per_million,
        };

        assert!(!excessive_base.is_within(&PaymentFee::SEND_FEE_LIMIT));

        let excessive_rate = PaymentFee {
            base: PaymentFee::SEND_FEE_LIMIT.base,
            parts_per_million: PaymentFee::SEND_FEE_LIMIT.parts_per_million + 1,
        };

        assert!(!excessive_rate.is_within(&PaymentFee::SEND_FEE_LIMIT));
    }

    #[test]
    fn a_summed_fee_is_bounded_by_the_send_limit() {
        // The gateway checks its lightning fee plus its transaction fee against the
        // send limit. Twenty thousand parts per million used to pass that check
        // because the summed base, well below the limit's base, decided it.
        let lightning_fee = PaymentFee {
            base: Amount::from_msats(20),
            parts_per_million: 20_000,
        };

        let send_fees = lightning_fee + PaymentFee::TRANSACTION_FEE_DEFAULT;

        assert!(send_fees.base < PaymentFee::SEND_FEE_LIMIT.base);
        assert!(!send_fees.is_within(&PaymentFee::SEND_FEE_LIMIT));
    }

    #[test]
    fn summing_fees_saturates_rather_than_wrapping_past_the_limit() {
        let extortionate = PaymentFee {
            base: Amount::from_msats(u64::MAX),
            parts_per_million: u64::MAX,
        };

        let send_fees = extortionate + PaymentFee::TRANSACTION_FEE_DEFAULT;

        assert_eq!(send_fees.base, Amount::from_msats(u64::MAX));
        assert_eq!(send_fees.parts_per_million, u64::MAX);
        assert!(!send_fees.is_within(&PaymentFee::SEND_FEE_LIMIT));
        assert_eq!(send_fees.fee(u64::MAX), Amount::from_msats(u64::MAX));
    }

    #[test]
    fn the_boundary_is_inclusive() {
        assert!(PaymentFee::SEND_FEE_LIMIT.is_within(&PaymentFee::SEND_FEE_LIMIT));
        assert!(PaymentFee::RECEIVE_FEE_LIMIT.is_within(&PaymentFee::RECEIVE_FEE_LIMIT));
    }

    #[test]
    fn is_within_is_amount_independent() {
        // These two fees swap places depending on how much is being paid, which is
        // why no total order on `PaymentFee` can express a limit.
        let flat = PaymentFee {
            base: Amount::from_sats(100),
            parts_per_million: 0,
        };

        let proportional = PaymentFee {
            base: Amount::ZERO,
            parts_per_million: 15_000,
        };

        assert!(flat.fee(1_000_000) > proportional.fee(1_000_000));
        assert!(flat.fee(100_000_000) < proportional.fee(100_000_000));

        // `is_within` is a componentwise predicate over the two fees alone, so it
        // returns one verdict for both — here, within the limit either way.
        assert!(flat.is_within(&PaymentFee::SEND_FEE_LIMIT));
        assert!(proportional.is_within(&PaymentFee::SEND_FEE_LIMIT));

        // And the confiscatory rate is rejected even though it costs nothing on a
        // payment of zero.
        assert_eq!(CONFISCATORY_RATE.fee(0), Amount::ZERO);
        assert!(!CONFISCATORY_RATE.is_within(&PaymentFee::SEND_FEE_LIMIT));
    }
}
