//! 本机目录维护和刷新分页的运行合同

use gateway_core::{
    account::{
        CredentialRevision, NewProviderAccount, PlaintextCredential, ProviderAccount,
        ProviderAccountId, ProviderAccountStore,
    },
    lifecycle::CancellationToken,
    routing::ProviderKind,
    task::{WorkerContribution, WorkerCycleContext, WorkerKind, WorkerRunnable},
};
use serde_json::json;
use std::{sync::Arc, time::SystemTime};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

#[tokio::test]
async fn refresh_skips_a_full_page_of_consumed_tokens_and_reaches_later_accounts() {
    let server = MockServer::start().await;
    let accounts = Arc::new(crate::support::MemoryProviderAccountStore::default());
    let token = crate::credential::token("app_fnr0pYvVwwFDocDumLG3H2Bp");
    for index in 0..101 {
        accounts.create_account(NewProviderAccount {
            account: ProviderAccount::new(
                ProviderAccountId::new(format!("acct_refresh-{index:03}")).unwrap(),
                ProviderKind::new("bps").unwrap(), "test".into(),
                Some("user-test".into()), "oauth".into(), CredentialRevision::new(1).unwrap(),
                Some(SystemTime::UNIX_EPOCH),
            ).with_profile(None, Some("workspace-test".into()), None)
             .with_refresh_schedule(true, None),
            model_access: None,
            credential: PlaintextCredential::new(json!({
                "access_token":token,"refresh_token":"test-refresh", "refresh_attempted":index < 100,
            }).as_object().unwrap().clone()),
        }).await.unwrap();
    }
    Mock::given(method("POST"))
        .and(path("/oauth/token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"access_token":token})))
        .expect(1)
        .mount(&server)
        .await;
    let mut bundle = provider_bps::initialize(
        provider_bps::BpsConfig {
            base_url: server.uri(),
            auth_base_url: server.uri(),
            quota_base_url: server.uri(),
            ..Default::default()
        },
        crate::support::ports_with(accounts.clone()),
    )
    .unwrap();
    let registration = bundle
        .take_worker_contributions()
        .into_iter()
        .find_map(|contribution| match contribution {
            WorkerContribution::Registration(registration)
                if registration.id.kind() == WorkerKind::OAuthRefresh =>
            {
                Some(registration)
            }
            _ => None,
        })
        .unwrap();
    let WorkerRunnable::Scheduled { lease, task, .. } = registration.runnable else {
        panic!("refresh must be scheduled")
    };
    assert!(lease.is_some());
    task.run_cycle(WorkerCycleContext::new(
        registration.id,
        None,
        CancellationToken::new(),
    ))
    .await
    .unwrap();
    let loaded = accounts
        .load_current_credential(&ProviderAccountId::new("acct_refresh-100").unwrap())
        .await
        .unwrap();
    assert_eq!(loaded.account.revision().get(), 3);
    assert!(
        loaded
            .credential
            .expose_to_provider()
            .get("refresh_attempted")
            .is_none()
    );
}

#[tokio::test]
async fn every_process_has_an_independent_catalog_worker() {
    let server = MockServer::start().await;
    let accounts = Arc::new(crate::support::MemoryProviderAccountStore::default());
    accounts
        .create_account(NewProviderAccount {
            account: ProviderAccount::new(
                ProviderAccountId::new("acct_catalog-worker").unwrap(),
                ProviderKind::new("bps").unwrap(),
                "test".into(),
                Some("user-test".into()),
                "oauth".into(),
                CredentialRevision::new(1).unwrap(),
                None,
            ),
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
    Mock::given(method("GET"))
        .and(path("/basispoints/api/responses/access"))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            json!({"allowed":true,"model_catalog":{"models":[{"id":"local-model"}]}}),
        ))
        .expect(2)
        .mount(&server)
        .await;
    // 两个 Bundle 代表两个副本，共享账号端口，但各自持有本机目录和代次
    for _ in 0..2 {
        let mut bundle = provider_bps::initialize(
            provider_bps::BpsConfig {
                base_url: server.uri(),
                auth_base_url: server.uri(),
                quota_base_url: server.uri(),
                ..Default::default()
            },
            crate::support::ports_with(accounts.clone()),
        )
        .unwrap();
        let provider = bundle.core_provider();
        let before = provider.catalog_generation();
        let registration = bundle
            .take_worker_contributions()
            .into_iter()
            .find_map(|contribution| match contribution {
                WorkerContribution::Registration(registration)
                    if registration.id.kind() == WorkerKind::QuotaCatalogHealth
                        && registration.id.owner() == "bps" =>
                {
                    Some(registration)
                }
                _ => None,
            })
            .expect("each process needs a catalog worker");
        let WorkerRunnable::Scheduled { lease, task, .. } = registration.runnable else {
            panic!("catalog must be scheduled")
        };
        assert!(
            lease.is_none(),
            "a local catalog must not wait for a cluster leader lease"
        );
        task.run_cycle(WorkerCycleContext::new(
            registration.id,
            None,
            CancellationToken::new(),
        ))
        .await
        .unwrap();
        assert_ne!(before, provider.catalog_generation());
        assert_eq!(provider.query_model_capabilities().await.unwrap().len(), 1);
    }
}

#[tokio::test]
async fn quota_worker_is_leased_and_refreshes_every_cycle_without_consuming_tokens() {
    let server = MockServer::start().await;
    let accounts = Arc::new(crate::support::MemoryProviderAccountStore::default());
    for (name, enabled, authentication) in [
        ("enabled", true, "oauth"),
        ("disabled", false, "oauth"),
        ("other", true, "api_key"),
    ] {
        accounts.create_account(NewProviderAccount {
            account: ProviderAccount::new(ProviderAccountId::new(format!("acct_quota-{name}")).unwrap(),ProviderKind::new("bps").unwrap(),name.into(),Some("user-test".into()),authentication.into(),CredentialRevision::new(1).unwrap(),None).with_profile(None,Some("workspace-test".into()),None).with_account_facts(enabled,gateway_core::account::CredentialState::Ready,gateway_core::account::QuotaState::unknown(),None,None),
            model_access:None,
            credential:PlaintextCredential::new(json!({"access_token":crate::credential::token("app_fnr0pYvVwwFDocDumLG3H2Bp"),"refresh_token":"must-not-consume"}).as_object().unwrap().clone()),
        }).await.unwrap();
    }
    Mock::given(method("GET")).and(path("/backend-api/wham/usage")).respond_with(ResponseTemplate::new(200).set_body_json(json!({"plan_type":"pro","rate_limit":{"allowed":true,"limit_reached":false,"primary_window":{"used_percent":25,"limit_window_seconds":18000,"reset_at":4102444800i64}}}))).expect(2).mount(&server).await;
    let mut bundle = provider_bps::initialize(
        provider_bps::BpsConfig {
            base_url: server.uri(),
            auth_base_url: server.uri(),
            quota_base_url: server.uri(),
            ..Default::default()
        },
        crate::support::ports_with(accounts.clone()),
    )
    .unwrap();
    let contributions = bundle.take_worker_contributions();
    assert_eq!(contributions.len(), 3);
    let registration = contributions
        .into_iter()
        .find_map(|contribution| match contribution {
            WorkerContribution::Registration(registration)
                if registration.id.owner() == "bps-quota" =>
            {
                Some(registration)
            }
            _ => None,
        })
        .unwrap();
    let WorkerRunnable::Scheduled {
        lease,
        task,
        schedule,
    } = registration.runnable
    else {
        panic!("quota must be scheduled")
    };
    assert!(lease.is_some());
    assert_eq!(schedule.interval(), std::time::Duration::from_secs(300));
    for _ in 0..2 {
        task.run_cycle(WorkerCycleContext::new(
            registration.id.clone(),
            None,
            CancellationToken::new(),
        ))
        .await
        .unwrap();
    }
    let quotas = accounts
        .get_quotas(&[ProviderAccountId::new("acct_quota-enabled").unwrap()])
        .await
        .unwrap();
    assert_eq!(quotas.len(), 1);
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    task.run_cycle(WorkerCycleContext::new(registration.id, None, cancellation))
        .await
        .unwrap();
    assert!(
        server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|request| request.method.as_str() == "GET")
    );
}
