//! HTTP 与 WS 认证、临时封禁和限流反馈在冷流中的持久边界
use futures::StreamExt;
use gateway_core::{
    account::{
        CredentialRevision, CredentialState, NewProviderAccount, PlaintextCredential,
        ProviderAccount, ProviderAccountId, ProviderAccountStore, QuotaState,
    },
    error::ProviderErrorKind,
    provider_ports::{ProviderCooldownKind, ProviderCooldownPort},
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
    fixture_with_transport(
        status,
        enabled,
        provider_bps::BpsUpstreamTransport::Sse,
        None,
    )
    .await
}

async fn fixture_with_transport(
    status: u16,
    enabled: bool,
    transport: provider_bps::BpsUpstreamTransport,
    event: Option<serde_json::Value>,
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
    Mock::given(method(
        if transport == provider_bps::BpsUpstreamTransport::Ws {
            "GET"
        } else {
            "POST"
        },
    ))
    .and(path("/basispoints/api/responses"))
    .respond_with(if let Some(event) = event {
        ResponseTemplate::new(200).set_body_raw(format!("data: {event}\n\n"), "text/event-stream")
    } else {
        ResponseTemplate::new(status)
            .insert_header("Retry-After", "17")
            .set_body_json(json!({"error":{"message":"rejected"}}))
    })
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
    let mut stream = crate::stream::stream_with_config(
        provider_bps::BpsConfig {
            base_url: server.uri(),
            upstream_transport: transport,
            ..Default::default()
        },
        "openai",
        ports,
        account,
        json!({"model":"model","input":"hello"}),
        gateway_core::engine::RequestAttemptContext::new(
            gateway_core::engine::ModelRequestId::new("req_feedback").unwrap(),
            gateway_core::policy::ClientApiKeyId::new("key_feedback").unwrap(),
        ),
        gateway_core::lifecycle::CancellationToken::new(),
    )
    .await
    .unwrap();
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
async fn forbidden_response_cools_enabled_accounts_for_six_hours_without_expiring_credentials() {
    for transport in [
        provider_bps::BpsUpstreamTransport::Sse,
        provider_bps::BpsUpstreamTransport::Ws,
    ] {
        for enabled in [true, false] {
            let before = SystemTime::now();
            let (_, accounts, cooldowns, id) =
                fixture_with_transport(403, enabled, transport, None).await;
            assert_eq!(
                accounts
                    .get_account(&id)
                    .await
                    .unwrap()
                    .unwrap()
                    .credential_state(),
                CredentialState::Ready
            );
            let cooldown = cooldowns.read(&id).await.unwrap();
            if enabled {
                let cooldown = cooldown.unwrap();
                assert_eq!(cooldown.kind(), ProviderCooldownKind::RateLimit);
                assert!(cooldown.until() >= before + Duration::from_secs(6 * 60 * 60));
                assert!(cooldown.until() <= SystemTime::now() + Duration::from_secs(6 * 60 * 60));
            } else {
                assert!(cooldown.is_none());
            }
        }
    }
}

#[tokio::test]
async fn forbidden_stream_error_cools_account_with_original_status() {
    let before = SystemTime::now();
    let event =
        json!({"type":"error","status":403,"error":{"code":"upstream_denied","message":"blocked"}});
    let (_, accounts, cooldowns, id) = fixture_with_transport(
        403,
        true,
        provider_bps::BpsUpstreamTransport::Sse,
        Some(event),
    )
    .await;
    let cooldown = cooldowns.read(&id).await.unwrap().unwrap();
    assert!(cooldown.until() >= before + Duration::from_secs(6 * 60 * 60));
    assert!(cooldown.until() <= SystemTime::now() + Duration::from_secs(6 * 60 * 60));
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
