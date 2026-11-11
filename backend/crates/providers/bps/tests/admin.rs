//! BPS 手工刷新与持久版本栅栏
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
                json!({"access_token":token,"refresh_token":"original-refresh","client_id":client})
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
    let material =
        crate::credential::document(json!({"access_token":crate::credential::token(client)}));
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
            provider_material: material,
        })
        .await
        .unwrap();
    assert_eq!(prepared.facts().plan_type.as_deref(), Some("pro"));
}
