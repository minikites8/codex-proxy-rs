//! 通过管理端公开能力验证 WHAM 配额与套餐的持久边界
use gateway_admin::{
    model::provider_credentials::{ProviderQuotaReadRequest, ProviderQuotaRequest},
    ports::provider::ProviderAdmin,
};
use gateway_core::{
    account::{
        CredentialRevision, CredentialState, NewProviderAccount, PlaintextCredential,
        ProviderAccount, ProviderAccountId, ProviderAccountStore, QuotaState,
    },
    routing::ProviderKind,
};
use serde_json::{Value, json};
use std::sync::Arc;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{header, method, path},
};

async fn fixture(
    server: &MockServer,
) -> (
    Arc<dyn ProviderAdmin>,
    Arc<crate::support::MemoryProviderAccountStore>,
    ProviderAccount,
) {
    fixture_enabled(server, true).await
}

async fn fixture_enabled(
    server: &MockServer,
    enabled: bool,
) -> (
    Arc<dyn ProviderAdmin>,
    Arc<crate::support::MemoryProviderAccountStore>,
    ProviderAccount,
) {
    let account = ProviderAccount::new(
        ProviderAccountId::new("acct_quota").unwrap(),
        ProviderKind::new("bps").unwrap(),
        "quota".into(),
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
    (provider, accounts, account)
}
fn usage() -> Value {
    json!({"account_id":"workspace-test","plan_type":"pro","rate_limit":{"allowed":true,"limit_reached":false,"primary_window":{"used_percent":32,"limit_window_seconds":604800,"reset_at":2100000000},"secondary_window":{"used_percent":14,"limit_window_seconds":18000,"reset_after_seconds":600}},"credits":{"has_credits":true,"unlimited":false,"balance":"12.34567890123456789"}})
}
async fn mount(server: &MockServer, value: Value) {
    Mock::given(method("GET"))
        .and(path("/backend-api/wham/usage"))
        .and(header("chatgpt-account-id", "workspace-test"))
        .and(header("openai-beta", "codex-1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(value))
        .mount(server)
        .await;
}
#[tokio::test]
async fn refresh_persists_plan_windows_and_exact_credits_and_batch_read_does_not_query() {
    let server = MockServer::start().await;
    mount(&server, usage()).await;
    let (provider, accounts, account) = fixture(&server).await;
    let result = provider
        .quota(ProviderQuotaRequest {
            account_id: account.id().clone(),
            refresh: true,
            rolling_usage: None,
        })
        .await
        .unwrap();
    assert_eq!(result.plan_type.as_deref(), Some("pro"));
    assert_eq!(
        result.credits.unwrap().balance.as_deref(),
        Some("12.34567890123456789")
    );
    assert!(
        result
            .windows
            .iter()
            .any(|window| window.key == "codex_7d" && window.used_percent == Some(32.0))
    );
    assert!(result.windows.iter().any(|window| window.key == "codex_5h"
        && window.used_percent == Some(14.0)
        && window.reset_at.is_some()));
    let current = accounts.get_account(account.id()).await.unwrap().unwrap();
    assert_eq!(current.plan_type(), Some("pro"));
    assert_eq!(current.credential_state(), CredentialState::Ready);
    let record = crate::support::account_record(&current);
    let views = provider
        .read_quotas(&[ProviderQuotaReadRequest {
            account: &record,
            rolling_usage: None,
        }])
        .await;
    assert_eq!(views[0].as_ref().unwrap().windows.len(), 2);
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
}
#[tokio::test]
async fn quota_permission_failure_preserves_credentials_and_previous_snapshot() {
    let server = MockServer::start().await;
    mount(&server, usage()).await;
    let (provider, accounts, account) = fixture(&server).await;
    provider
        .quota(ProviderQuotaRequest {
            account_id: account.id().clone(),
            refresh: true,
            rolling_usage: None,
        })
        .await
        .unwrap();
    server.reset().await;
    Mock::given(method("GET"))
        .and(path("/backend-api/wham/usage"))
        .respond_with(ResponseTemplate::new(401))
        .mount(&server)
        .await;
    assert!(
        provider
            .quota(ProviderQuotaRequest {
                account_id: account.id().clone(),
                refresh: true,
                rolling_usage: None
            })
            .await
            .is_err()
    );
    let current = accounts.get_account(account.id()).await.unwrap().unwrap();
    assert_eq!(current.credential_state(), CredentialState::Ready);
    let result = provider
        .quota(ProviderQuotaRequest {
            account_id: account.id().clone(),
            refresh: false,
            rolling_usage: None,
        })
        .await
        .unwrap();
    assert_eq!(result.plan_type.as_deref(), Some("pro"));
    assert_eq!(result.windows.len(), 2);
}
#[tokio::test]
async fn missing_or_invalid_window_usage_does_not_become_free_or_zero() {
    let server = MockServer::start().await;
    mount(&server,json!({"plan_type":"plus","rate_limit":{"allowed":true,"primary_window":{"used_percent":-1,"limit_window_seconds":18000},"secondary_window":null},"credits":null})).await;
    let (provider, _, account) = fixture(&server).await;
    let result = provider
        .quota(ProviderQuotaRequest {
            account_id: account.id().clone(),
            refresh: true,
            rolling_usage: None,
        })
        .await
        .unwrap();
    assert!(result.credits.is_none());
    assert!(
        result
            .windows
            .iter()
            .all(|window| window.used_percent.is_none())
    );
    assert!(!result.limit_reached);
}

#[tokio::test]
async fn partial_headers_preserve_known_window_and_do_not_release_exhausted_account() {
    use futures::StreamExt;
    let server = MockServer::start().await;
    let mut exhausted = usage();
    exhausted["rate_limit"]["allowed"] = false.into();
    exhausted["rate_limit"]["limit_reached"] = true.into();
    exhausted["rate_limit"]["primary_window"]["used_percent"] = 100.into();
    mount(&server, exhausted).await;
    let (provider, accounts, account) = fixture(&server).await;
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
            ResponseTemplate::new(200)
                .insert_header("x-codex-secondary-used-percent", "15")
                .set_body_raw("data: invalid-json\n\n", "text/event-stream"),
        )
        .mount(&server)
        .await;
    // 使用原始已冻结账号，模拟已发出的请求在耗尽快照之后才收到部分响应头
    let mut stream = crate::stream::stream_with_ports(
        &server,
        "openai",
        Default::default(),
        crate::support::ports_with(accounts.clone()),
        account.clone(),
    )
    .await;
    provider
        .quota(ProviderQuotaRequest {
            account_id: account.id().clone(),
            refresh: true,
            rolling_usage: None,
        })
        .await
        .unwrap();
    assert!(stream.next().await.unwrap().is_err());
    let result = provider
        .quota(ProviderQuotaRequest {
            account_id: account.id().clone(),
            refresh: false,
            rolling_usage: None,
        })
        .await
        .unwrap();
    assert!(result.windows.iter().any(|window| window.key == "codex_7d"
        && window.used_percent == Some(100.0)
        && window.reset_at.is_some()));
    assert!(result.windows.iter().any(|window| window.key == "codex_5h"
        && window.used_percent == Some(15.0)
        && window.reset_at.is_some()));
    assert_eq!(
        accounts
            .get_account(account.id())
            .await
            .unwrap()
            .unwrap()
            .quota()
            .access(),
        gateway_core::account::QuotaAccessState::Exhausted
    );
}

#[tokio::test]
async fn fresh_complete_wham_can_recover_exhausted_quota_without_changing_credential() {
    let server = MockServer::start().await;
    let mut exhausted = usage();
    exhausted["rate_limit"]["allowed"] = false.into();
    exhausted["rate_limit"]["limit_reached"] = true.into();
    exhausted["rate_limit"]["primary_window"]["used_percent"] = 100.into();
    mount(&server, exhausted).await;
    let (provider, accounts, account) = fixture(&server).await;
    provider
        .quota(ProviderQuotaRequest {
            account_id: account.id().clone(),
            refresh: true,
            rolling_usage: None,
        })
        .await
        .unwrap();
    server.reset().await;
    mount(&server, usage()).await;
    provider
        .quota(ProviderQuotaRequest {
            account_id: account.id().clone(),
            refresh: true,
            rolling_usage: None,
        })
        .await
        .unwrap();
    let current = accounts.get_account(account.id()).await.unwrap().unwrap();
    assert_eq!(
        current.quota().access(),
        gateway_core::account::QuotaAccessState::Allowed
    );
    assert_eq!(current.credential_state(), CredentialState::Ready);
}

#[tokio::test]
async fn slow_older_wham_query_cannot_replace_newer_response_header() {
    use futures::StreamExt;
    let server = MockServer::start().await;
    use std::{
        io::{Read, Write},
        net::TcpListener,
        sync::mpsc,
        time::Duration,
    };
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let quota_uri = format!("http://{}", listener.local_addr().unwrap());
    let started = Arc::new(tokio::sync::Notify::new());
    let observed = started.clone();
    let (release, gate) = mpsc::channel();
    // 明确等待新响应头已提交，再释放旧查询，不依赖固定延迟
    let quota_server = std::thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let mut request = Vec::new();
        while !request.ends_with(b"\r\n\r\n") {
            let mut byte = [0];
            assert_eq!(socket.read(&mut byte).unwrap(), 1);
            request.push(byte[0]);
        }
        observed.notify_one();
        gate.recv_timeout(Duration::from_secs(10)).unwrap();
        let body = usage().to_string();
        write!(socket,"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",body.len(),body).unwrap();
    });
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
            ResponseTemplate::new(200)
                .insert_header("x-codex-primary-used-percent", "90")
                .insert_header("x-codex-primary-window-minutes", "10080")
                .set_body_raw("data: invalid-json\n\n", "text/event-stream"),
        )
        .mount(&server)
        .await;
    let (_, accounts, account) = fixture(&server).await;
    let provider = provider_bps::initialize(
        provider_bps::BpsConfig {
            base_url: server.uri(),
            auth_base_url: server.uri(),
            quota_base_url: quota_uri,
            ..Default::default()
        },
        crate::support::ports_with(accounts.clone()),
    )
    .unwrap()
    .admin_provider();
    let querying = provider.clone();
    let id = account.id().clone();
    let pending = tokio::spawn(async move {
        querying
            .quota(ProviderQuotaRequest {
                account_id: id,
                refresh: true,
                rolling_usage: None,
            })
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), started.notified())
        .await
        .unwrap();
    let mut stream = crate::stream::stream_with_ports(
        &server,
        "openai",
        Default::default(),
        crate::support::ports_with(accounts.clone()),
        account.clone(),
    )
    .await;
    assert!(stream.next().await.unwrap().is_err());
    release.send(()).unwrap();
    let returned = pending.await.unwrap().unwrap();
    assert!(
        returned
            .windows
            .iter()
            .any(|window| window.key == "codex_7d" && window.used_percent == Some(90.0))
    );
    let result = provider
        .quota(ProviderQuotaRequest {
            account_id: account.id().clone(),
            refresh: false,
            rolling_usage: None,
        })
        .await
        .unwrap();
    assert!(
        result
            .windows
            .iter()
            .any(|window| window.key == "codex_7d" && window.used_percent == Some(90.0))
    );
    quota_server.join().unwrap();
}

#[tokio::test]
async fn disabled_account_diagnostic_does_not_persist_passive_quota() {
    use futures::StreamExt;
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
            ResponseTemplate::new(200)
                .insert_header("x-codex-primary-used-percent", "90")
                .set_body_raw("data: invalid-json\n\n", "text/event-stream"),
        )
        .mount(&server)
        .await;
    let (_, accounts, account) = fixture_enabled(&server, false).await;
    let mut stream = crate::stream::stream_with_ports(
        &server,
        "openai",
        Default::default(),
        crate::support::ports_with(accounts.clone()),
        account.clone(),
    )
    .await;
    assert!(stream.next().await.unwrap().is_err());
    assert!(
        accounts
            .get_quotas(&[account.id().clone()])
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn local_rolling_usage_is_independent_of_shared_quota_and_survives_all_read_paths() {
    let server = MockServer::start().await;
    let (provider, accounts, account) = fixture(&server).await;
    let local = gateway_admin::model::accounts::AccountUsage {
        account_id: account.id().as_str().to_owned(),
        request_count: 2,
        success_count: 2,
        input_tokens: Some(100),
        output_tokens: Some(20),
        cached_input_tokens: None,
        cache_write_input_tokens: None,
        reasoning_output_tokens: None,
        image_input_tokens: None,
        image_output_tokens: None,
        image_request_count: 0,
        image_request_failed_count: 0,
        total_tokens: Some(120),
        cost_coverage: Default::default(),
        costs: Vec::new(),
        last_used_at: None,
        request_buckets: Vec::new(),
        models: Vec::new(),
    };
    for refresh in [false, true, false] {
        if refresh {
            mount(&server, usage()).await;
        }
        let quota = provider
            .quota(ProviderQuotaRequest {
                account_id: account.id().clone(),
                refresh,
                rolling_usage: Some(local.clone()),
            })
            .await
            .unwrap();
        let (window, _) = quota.usage_window().unwrap();
        assert_eq!(window.local_usage.as_ref(), Some(&local));
        assert_eq!(window.window_seconds, Some(86_400));
        assert!(window.used_percent.is_none());
        assert!(window.reset_at.is_none());
        assert!(
            quota
                .windows
                .iter()
                .filter(|window| window.limit_id.is_some())
                .all(|window| window.local_usage.is_none())
        );
    }
    let current = accounts.get_account(account.id()).await.unwrap().unwrap();
    let record = crate::support::account_record(&current);
    let quotas = provider
        .read_quotas(&[ProviderQuotaReadRequest {
            account: &record,
            rolling_usage: Some(&local),
        }])
        .await;
    assert_eq!(
        quotas[0]
            .as_ref()
            .unwrap()
            .usage_window()
            .unwrap()
            .0
            .local_usage
            .as_ref(),
        Some(&local)
    );
    let missing = provider
        .quota(ProviderQuotaRequest {
            account_id: account.id().clone(),
            refresh: false,
            rolling_usage: None,
        })
        .await
        .unwrap();
    assert!(missing.usage_window().is_none());
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
}
