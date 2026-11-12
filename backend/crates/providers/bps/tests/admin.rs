//! BPS 账号连接配置、手工刷新与持久版本栅栏
use gateway_admin::model::provider_credentials::{
    PrepareCredentialRefresh, PrepareCredentialRotation,
};
use gateway_core::account::{
    CredentialRevision, NewProviderAccount, PlaintextCredential, ProviderAccount,
    ProviderAccountId, ProviderAccountStore,
};
use gateway_core::routing::ProviderKind;
use serde_json::json;
use std::sync::Arc;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path, query_param},
};
#[tokio::test]
async fn refresh_preserves_refresh_token_and_fences_prepared_commit() {
    let server = MockServer::start().await;
    let accounts = Arc::new(crate::support::MemoryProviderAccountStore::default());
    let client = "app_fnr0pYvVwwFDocDumLG3H2Bp";
    let token = crate::credential::token(client);
    let id = ProviderAccountId::new("acct_bps-refresh-test").unwrap();
    let account = ProviderAccount::new(
        id.clone(),
        ProviderKind::new("bps").unwrap(),
        "test".into(),
        Some("user-test".into()),
        "oauth".into(),
        CredentialRevision::new(1).unwrap(),
        None,
    )
    .with_profile(None, Some("workspace-test".into()), None);
    accounts
        .create_account(NewProviderAccount {
            account: account.clone(),
            model_access: None,
            credential: PlaintextCredential::new(
                json!({"access_token":token,"refresh_token":"original-refresh","client_id":client,"transport":"http","image_input_enabled":false,"omit_images_when_disabled":true,"omit_unsupported_hosted_tools":true,"ignore_encrypted_history":true})
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        })
        .await
        .unwrap();
    let provider = provider_bps::initialize(
        provider_bps::BpsConfig {
            base_url: server.uri(),
            auth_base_url: server.uri(),
            quota_base_url: server.uri(),
            ..Default::default()
        },
        crate::support::ports_with(accounts.clone()),
    )
    .unwrap()
    .admin_provider();
    Mock::given(method("POST"))
        .and(path("/oauth/token"))
        .and(query_param("unified", "true"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"access_token":crate::credential::token(client)})),
        )
        .expect(1)
        .mount(&server)
        .await;
    let prepared = provider
        .prepare_refresh(PrepareCredentialRefresh {
            account: crate::support::account_record(&account),
        })
        .await
        .unwrap();
    assert_eq!(prepared.facts().expected_credential_revision.get(), 2);
    assert_eq!(
        prepared
            .facts()
            .provider_material
            .expose_to_provider()
            .expose_to_provider()["transport"],
        "http"
    );
    let options = prepared
        .facts()
        .provider_material
        .expose_to_provider()
        .expose_to_provider();
    assert_eq!(options["image_input_enabled"], false);
    assert_eq!(options["omit_images_when_disabled"], true);
    assert_eq!(options["omit_unsupported_hosted_tools"], true);
    assert_eq!(options["ignore_encrypted_history"], true);
    assert_eq!(
        prepared
            .facts()
            .provider_material
            .expose_to_provider()
            .expose_to_provider()["refresh_token"],
        "original-refresh"
    );
    let persisted = accounts.load_current_credential(&id).await.unwrap();
    assert_eq!(
        persisted.credential.expose_to_provider()["refresh_attempted"],
        true
    );
    assert!(
        provider
            .prepare_refresh(PrepareCredentialRefresh {
                account: crate::support::account_record(&persisted.account)
            })
            .await
            .is_err()
    );
}

#[tokio::test]
async fn cancelling_refresh_after_token_send_does_not_replay_the_refresh_token() {
    let server = MockServer::start().await;
    let accounts = Arc::new(crate::support::MemoryProviderAccountStore::default());
    let client = "app_fnr0pYvVwwFDocDumLG3H2Bp";
    let id = ProviderAccountId::new("acct_bps-cancel-refresh").unwrap();
    let account = ProviderAccount::new(
        id.clone(),
        ProviderKind::new("bps").unwrap(),
        "test".into(),
        Some("user-test".into()),
        "oauth".into(),
        CredentialRevision::new(1).unwrap(),
        None,
    )
    .with_profile(None, Some("workspace-test".into()), None)
    .with_account_facts(
        true,
        gateway_core::account::CredentialState::Expired,
        gateway_core::account::QuotaState::unknown(),
        None,
        None,
    );
    accounts.create_account(NewProviderAccount {account:account.clone(),model_access:None,credential:PlaintextCredential::new(json!({"access_token":crate::credential::token(client),"refresh_token":"original-refresh","client_id":client}).as_object().unwrap().clone())}).await.unwrap();
    let provider = provider_bps::initialize(
        provider_bps::BpsConfig {
            base_url: server.uri(),
            auth_base_url: server.uri(),
            quota_base_url: server.uri(),
            ..Default::default()
        },
        crate::support::ports_with(accounts.clone()),
    )
    .unwrap()
    .admin_provider();
    Mock::given(method("POST"))
        .and(path("/oauth/token"))
        .and(query_param("unified", "true"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_delay(std::time::Duration::from_secs(30))
                .set_body_json(json!({"access_token":crate::credential::token(client)})),
        )
        .expect(1)
        .mount(&server)
        .await;
    let task = {
        let provider = provider.clone();
        let account = account.clone();
        tokio::spawn(async move {
            provider
                .prepare_refresh(PrepareCredentialRefresh {
                    account: crate::support::account_record(&account),
                })
                .await
        })
    };
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if !server.received_requests().await.unwrap().is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("refresh must reach token endpoint");
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    let loaded = accounts.load_current_credential(&id).await.unwrap();
    assert_eq!(
        loaded.account.credential_state(),
        gateway_core::account::CredentialState::Expired
    );
    assert!(
        provider
            .prepare_refresh(PrepareCredentialRefresh {
                account: crate::support::account_record(&loaded.account)
            })
            .await
            .is_err()
    );
}

#[tokio::test]
async fn rotation_keeps_existing_plan_when_new_tokens_omit_plan_claim() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/basispoints/api/responses/access"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"allowed":true,"model_catalog":{"models":[]}})),
        )
        .mount(&server)
        .await;
    let accounts = Arc::new(crate::support::MemoryProviderAccountStore::default());
    let client = "app_fnr0pYvVwwFDocDumLG3H2Bp";
    let account = ProviderAccount::new(
        ProviderAccountId::new("acct_plan_rotation").unwrap(),
        ProviderKind::new("bps").unwrap(),
        "plan".into(),
        Some("user-test".into()),
        "oauth".into(),
        CredentialRevision::new(1).unwrap(),
        None,
    )
    .with_profile(None, Some("workspace-test".into()), Some("pro".into()));
    let material = crate::credential::document(
        json!({"access_token":crate::credential::token(client),"transport":"http","image_input_enabled":false,"omit_images_when_disabled":true,"omit_unsupported_hosted_tools":true,"ignore_encrypted_history":true}),
    );
    accounts
        .create_account(NewProviderAccount {
            account: account.clone(),
            model_access: None,
            credential: PlaintextCredential::new(
                material.expose_to_provider().expose_to_provider().clone(),
            ),
        })
        .await
        .unwrap();
    let provider = provider_bps::initialize(
        provider_bps::BpsConfig {
            base_url: server.uri(),
            auth_base_url: server.uri(),
            quota_base_url: server.uri(),
            ..Default::default()
        },
        crate::support::ports_with(accounts),
    )
    .unwrap()
    .admin_provider();
    let prepared = provider
        .prepare_rotation(PrepareCredentialRotation {
            account: crate::support::account_record(&account),
            provider_material: crate::credential::document(
                json!({"access_token":crate::credential::token(client)}),
            ),
        })
        .await
        .unwrap();
    assert_eq!(prepared.facts().plan_type.as_deref(), Some("pro"));
    assert_eq!(
        prepared
            .facts()
            .provider_material
            .expose_to_provider()
            .expose_to_provider()["transport"],
        "http"
    );
    let options = prepared
        .facts()
        .provider_material
        .expose_to_provider()
        .expose_to_provider();
    assert_eq!(options["image_input_enabled"], false);
    assert_eq!(options["omit_images_when_disabled"], true);
    assert_eq!(options["omit_unsupported_hosted_tools"], true);
    assert_eq!(options["ignore_encrypted_history"], true);
}

#[tokio::test]
async fn account_transport_configuration_uses_default_and_prepares_isolated_updates() {
    for (default, expected) in [
        (provider_bps::BpsUpstreamTransport::Ws, "prefer_websocket"),
        (provider_bps::BpsUpstreamTransport::Sse, "http"),
    ] {
        let accounts = Arc::new(crate::support::MemoryProviderAccountStore::default());
        let account = ProviderAccount::new(
            ProviderAccountId::new("acct_transport").unwrap(),
            ProviderKind::new("bps").unwrap(),
            "transport".into(),
            Some("user-test".into()),
            "oauth".into(),
            CredentialRevision::new(1).unwrap(),
            None,
        )
        .with_profile(None, Some("workspace-test".into()), Some("pro".into()));
        let material = json!({"access_token":crate::credential::token("app_fnr0pYvVwwFDocDumLG3H2Bp"),"refresh_token":"unchanged-refresh","refresh_attempted":true,"client_id":"app_fnr0pYvVwwFDocDumLG3H2Bp","chatgpt_account_id":"workspace-test","schema_version":1});
        accounts
            .create_account(NewProviderAccount {
                account: account.clone(),
                model_access: None,
                credential: PlaintextCredential::new(material.as_object().unwrap().clone()),
            })
            .await
            .unwrap();
        let provider = provider_bps::initialize(
            provider_bps::BpsConfig {
                upstream_transport: default,
                ..Default::default()
            },
            crate::support::ports_with(accounts.clone()),
        )
        .unwrap()
        .admin_provider();
        let configuration = provider
            .account_configuration(account.id())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            configuration.expose_to_provider().expose_to_provider(),
            json!({"transport":expected,"image_input_enabled":true,"omit_images_when_disabled":false,"omit_unsupported_hosted_tools":false,"ignore_encrypted_history":false}).as_object().unwrap()
        );
        for transport in ["http", "prefer_websocket"] {
            let prepared = provider
                .prepare_rotation(PrepareCredentialRotation {
                    account: crate::support::account_record(&account),
                    provider_material: crate::credential::document(json!({"transport":transport,"image_input_enabled":false,"omit_images_when_disabled":true,"omit_unsupported_hosted_tools":true,"ignore_encrypted_history":true})),
                })
                .await
                .unwrap();
            let facts = prepared.facts();
            assert!(facts.preserve_profile && facts.preserve_credential_state);
            assert_eq!(facts.replacement_identity, None);
            let mut expected_material = material.clone();
            expected_material["transport"] = transport.into();
            expected_material["image_input_enabled"] = false.into();
            expected_material["omit_images_when_disabled"] = true.into();
            expected_material["omit_unsupported_hosted_tools"] = true.into();
            expected_material["ignore_encrypted_history"] = true.into();
            assert_eq!(
                facts
                    .provider_material
                    .expose_to_provider()
                    .expose_to_provider(),
                expected_material.as_object().unwrap()
            );
            assert_eq!(
                accounts
                    .load_current_credential(account.id())
                    .await
                    .unwrap()
                    .credential
                    .expose_to_provider(),
                material.as_object().unwrap(),
                "preparation must not persist before the account transaction"
            );
        }
        for invalid in [
            json!({"transport":"invalid"}),
            json!({"transport":"http","image_input_enabled":"invalid"}),
            json!({"transport":"http","api_key":"unexpected"}),
            json!({"transport":"http","access_token":"unexpected"}),
        ] {
            assert!(
                provider
                    .prepare_rotation(PrepareCredentialRotation {
                        account: crate::support::account_record(&account),
                        provider_material: crate::credential::document(invalid),
                    })
                    .await
                    .is_err()
            );
        }
        let mut stale = crate::support::account_record(&account);
        stale.credential_revision = gateway_admin::model::Revision::new(2).unwrap();
        assert!(
            provider
                .prepare_rotation(PrepareCredentialRotation {
                    account: stale,
                    provider_material: crate::credential::document(json!({"transport":"http"})),
                })
                .await
                .is_err()
        );
    }
}
