//! Conversions between protobuf and Spider core resource group types.

use spider_core::types::resource_group::ExternalResourceGroupCredentials;

use crate::storage;

impl From<&ExternalResourceGroupCredentials> for storage::ExternalResourceGroupCredentials {
    fn from(credentials: &ExternalResourceGroupCredentials) -> Self {
        Self {
            external_resource_group_id: credentials.get_external_resource_group_id().to_owned(),
            password: credentials.get_password().to_vec(),
        }
    }
}

impl From<storage::ExternalResourceGroupCredentials> for ExternalResourceGroupCredentials {
    fn from(credentials: storage::ExternalResourceGroupCredentials) -> Self {
        Self::new(credentials.external_resource_group_id, credentials.password)
    }
}

#[cfg(test)]
mod tests {
    use prost::Message;
    use spider_core::types::resource_group::ExternalResourceGroupCredentials;

    use crate::storage;

    #[test]
    fn test_external_resource_group_credentials_protocol_round_trip() {
        let credentials = ExternalResourceGroupCredentials::new(
            "external-resource-group".to_owned(),
            vec![0, 1, 2, 255],
        );

        let encoded = storage::ExternalResourceGroupCredentials::from(&credentials).encode_to_vec();
        let decoded = ExternalResourceGroupCredentials::from(
            storage::ExternalResourceGroupCredentials::decode(encoded.as_slice())
                .expect("external resource group credentials should decode"),
        );

        assert_eq!(
            decoded.get_external_resource_group_id(),
            credentials.get_external_resource_group_id()
        );
        assert_eq!(decoded.get_password(), credentials.get_password());
    }
}
