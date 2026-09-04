/// The SP4 seam. `logweir-aws` will implement this over the Apache-2.0
/// `aws-msk-iam-sasl-signer` crate behind the non-default `msk-iam` feature;
/// v0.1 declares the trait and ships no implementation, which is what keeps
/// the pure-layer rule true (spec §6 C2).
pub trait TokenProvider: std::fmt::Debug + Send + Sync {
    /// The broker host and port are both passed because a mechanism may include
    /// the port in a signed payload (the MSK IAM presigner does).
    fn token(&self, broker_host: &str, broker_port: u16) -> Result<String, String>;
}
