//! Conversions between protobuf and Spider core resource group types.

use spider_core::types::resource_group::ExternalResourceGroupCredentials;

use crate::storage;

impl From<ExternalResourceGroupCredentials> for storage::ExternalResourceGroupCredentials {
    fn from(credentials: ExternalResourceGroupCredentials) -> Self {
        Self {
            external_resource_group_id: credentials.external_resource_group_id,
            password: credentials.password,
        }
    }
}

impl From<storage::ExternalResourceGroupCredentials> for ExternalResourceGroupCredentials {
    fn from(credentials: storage::ExternalResourceGroupCredentials) -> Self {
        Self {
            external_resource_group_id: credentials.external_resource_group_id,
            password: credentials.password,
        }
    }
}

#[cfg(test)]
mod tests {
    use prost::Message;
    use spider_core::types::resource_group::ExternalResourceGroupCredentials;

    use crate::storage;

    #[test]
    fn test_external_resource_group_credentials_protocol_round_trip() {
        let credentials = ExternalResourceGroupCredentials {
            external_resource_group_id: "external-resource-group".to_owned(),
            password: vec![0, 1, 2, 255],
        };

        let encoded =
            storage::ExternalResourceGroupCredentials::from(credentials.clone()).encode_to_vec();
        let decoded = ExternalResourceGroupCredentials::from(
            storage::ExternalResourceGroupCredentials::decode(encoded.as_slice())
                .expect("external resource group credentials should decode"),
        );

        assert_eq!(decoded, credentials);
    }
}
