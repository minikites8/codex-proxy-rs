//! HTTP 认证失败与限流反馈在真实冷流中的持久边界
use futures::StreamExt;
use gateway_core::{
    account::{
        CredentialRevision, CredentialState, NewProviderAccount, PlaintextCredential,
        ProviderAccount, ProviderAccountId, ProviderAccountStore, QuotaState,
    },
    error::ProviderErrorKind,
    provider_ports::ProviderCooldownPort,
    routing::ProviderKind,
};
use serde_json::json;
use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

async fn fixture(
    status: u16,
    enabled: bool,
) -> (
    gateway_core::engine::provider::ProviderStream,
    Arc<crate::support::MemoryProviderAccountStore>,
    Arc<dyn ProviderCooldownPort>,
    ProviderAccountId,
) {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/basispoints/api/responses/access"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"allowed":true,"model_catalog":{"models":[{"id":"model"}]}})),
        )
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/basispoints/api/responses"))
        .respond_with(
            ResponseTemplate::new(status)
                .insert_header("Retry-After", "17")
                .set_body_json(json!({"error":{"message":"rejected"}})),
        )
        .mount(&server)
        .await;
    let account = ProviderAccount::new(
        ProviderAccountId::new("acct_feedback").unwrap(),
        ProviderKind::new("bps").unwrap(),
        "feedback".into(),
        Some("user-test".into()),
        "oauth".into(),
        CredentialRevision::new(1).unwrap(),
        None,
    )
    .with_profile(None, Some("workspace-test".into()), None)
    .with_account_facts(
        enabled,
        CredentialState::Ready,
        QuotaState::unknown(),
        None,
        None,
    );
    let id = account.id().clone();
    let accounts = Arc::new(crate::support::MemoryProviderAccountStore::default());
    accounts
        .create_account(NewProviderAccount {
            account: account.clone(),
            model_access: None,
            credential: PlaintextCredential::new(
                json!({"access_token":crate::credential::token("app_fnr0pYvVwwFDocDumLG3H2Bp")})
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        })
        .await
        .unwrap();
    let ports = crate::support::ports_with(accounts.clone());
    let cooldown = ports.cooldowns();
    let mut stream =
        crate::stream::stream_with_ports(&server, "openai", Default::default(), ports, account)
            .await;
    // MockServer 必须覆盖 cold stream 的发送窗口
    let error = stream.next().await.unwrap().unwrap_err();
    assert_eq!(error.upstream_status(), Some(status));
    if status == 403 {
        assert_eq!(error.kind(), ProviderErrorKind::PermissionDenied);
    }
    (stream, accounts, cooldown, id)
}
#[tokio::test]
async fn authentication_rejection_expires_only_enabled_account() {
    for enabled in [true, false] {
        let (_, accounts, _, id) = fixture(401, enabled).await;
        assert_eq!(
            accounts
                .get_account(&id)
                .await
                .unwrap()
                .unwrap()
                .credential_state(),
            if enabled {
                CredentialState::Expired
            } else {
                CredentialState::Ready
            }
        );
    }
}
#[tokio::test]
async fn rate_limit_sets_bounded_cooldown_without_invalidating_credential() {
    let before = SystemTime::now();
    let (_, accounts, cooldowns, id) = fixture(429, true).await;
    let cooldown = cooldowns.read(&id).await.unwrap().unwrap();
    assert!(cooldown.until() >= before + Duration::from_secs(17));
    assert!(cooldown.until() <= SystemTime::now() + Duration::from_secs(17));
    assert_eq!(
        accounts
            .get_account(&id)
            .await
            .unwrap()
            .unwrap()
            .credential_state(),
        CredentialState::Ready
    );
}
#[tokio::test]
async fn forbidden_response_does_not_disable_account() {
    let (_, accounts, cooldowns, id) = fixture(403, true).await;
    assert_eq!(
        accounts
            .get_account(&id)
            .await
            .unwrap()
            .unwrap()
            .credential_state(),
        CredentialState::Ready
    );
    assert!(cooldowns.read(&id).await.unwrap().is_none());
}
