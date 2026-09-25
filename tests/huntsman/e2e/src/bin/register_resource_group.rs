//! Registers a resource group with the Spider deployment and prints its assigned id.
//!
//! A resource group must exist before a dedicated worker pool can bind to it, so the e2e harness
//! pre-registers it with this helper and seeds the returned id into the test driver via
//! `SPIDER_E2E_PREREGISTERED_RESOURCE_GROUPS`.

use anyhow::Context;
use spider_client::SpiderClient;
use spider_core::types::resource_group::ExternalResourceGroupCredentials;
use tonic::transport::Endpoint;

/// Registers the resource group named by the first CLI argument (with an empty password) against
/// the `SPIDER_ENDPOINT` deployment, printing its Spider-assigned id to stdout.
///
/// # Errors
///
/// Returns an error if:
///
/// * [`anyhow::Error`] if `SPIDER_ENDPOINT` is unset or the external-id argument is missing.
/// * Forwards [`Endpoint::from_shared`]'s return values on failure.
/// * Forwards [`spider_client::SpiderClientBuilder::connect`]'s return values on failure.
/// * Forwards [`SpiderClient::add_resource_group`]'s return values on failure.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let endpoint = std::env::var("SPIDER_ENDPOINT").context("SPIDER_ENDPOINT is not set")?;
    let external_resource_group_id = std::env::args()
        .nth(1)
        .context("missing external resource-group id argument")?;
    let client = SpiderClient::builder(Endpoint::from_shared(endpoint)?)
        .connect()
        .await?;
    let resource_group_id = client
        .add_resource_group(ExternalResourceGroupCredentials::new(
            external_resource_group_id,
            Vec::new(),
        ))
        .await?;
    println!("{}", resource_group_id.get());
    Ok(())
}
