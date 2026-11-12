//! 目录失败与权威空目录必须保持不同语义

use gateway_core::{
    account::{
        CredentialRevision, NewProviderAccount, PlaintextCredential, ProviderAccount,
        ProviderAccountId, ProviderAccountStore,
    },
    routing::ProviderKind,
};
use serde_json::json;
use std::sync::Arc;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{header, method, path},
};

async fn add_account(accounts: &crate::support::MemoryProviderAccountStore, id: &str, token: &str) {
    accounts
        .create_account(NewProviderAccount {
            account: ProviderAccount::new(
                ProviderAccountId::new(id).unwrap(),
                ProviderKind::new("bps").unwrap(),
                id.into(),
                Some("user-test".into()),
                "oauth".into(),
                CredentialRevision::new(1).unwrap(),
                None,
            ),
            model_access: None,
            credential: PlaintextCredential::new(
                json!({"access_token":token}).as_object().unwrap().clone(),
            ),
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn one_failed_account_does_not_publish_a_partial_authoritative_catalog() {
    let server = MockServer::start().await;
    let accounts = Arc::new(crate::support::MemoryProviderAccountStore::default());
    let first = format!(
        "{}-first",
        crate::credential::token("app_fnr0pYvVwwFDocDumLG3H2Bp")
    );
    let second = format!(
        "{}-second",
        crate::credential::token("app_fnr0pYvVwwFDocDumLG3H2Bp")
    );
    add_account(&accounts, "acct_catalog-first", &first).await;
    add_account(&accounts, "acct_catalog-second", &second).await;
    Mock::given(method("GET"))
        .and(path("/basispoints/api/responses/access"))
        .and(header("authorization", format!("Bearer {first}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            json!({"allowed":true,"model_catalog":{"models":[{"id":"visible-model"}]}}),
        ))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/basispoints/api/responses/access"))
        .and(header("authorization", format!("Bearer {second}")))
        .respond_with(ResponseTemplate::new(503))
        .expect(1)
        .mount(&server)
        .await;
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
    .core_provider();
    assert!(provider.query_model_capabilities().await.is_err());
}

#[tokio::test]
async fn authoritative_access_denial_publishes_an_empty_catalog() {
    let server = MockServer::start().await;
    let accounts = Arc::new(crate::support::MemoryProviderAccountStore::default());
    add_account(
        &accounts,
        "acct_catalog-denied",
        &crate::credential::token("app_fnr0pYvVwwFDocDumLG3H2Bp"),
    )
    .await;
    Mock::given(method("GET"))
        .and(path("/basispoints/api/responses/access"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"allowed":false})))
        .expect(1)
        .mount(&server)
        .await;
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
    .core_provider();
    assert!(
        provider
            .query_model_capabilities()
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn vision_capabilities_follow_account_options_over_global_defaults() {
    use gateway_core::operation::{CapabilityRequirements, Feature, OperationKind};
    for (enabled, omit) in [(false, false), (false, true), (true, false)] {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/basispoints/api/responses/access"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(
                    json!({"allowed":true,"model_catalog":{"models":[{"id":"model"}]}}),
                ),
            )
            .mount(&server)
            .await;
        let accounts = Arc::new(crate::support::MemoryProviderAccountStore::default());
        let id = ProviderAccountId::new("acct_vision_options").unwrap();
        add_account(
            &accounts,
            id.as_str(),
            &crate::credential::token("app_fnr0pYvVwwFDocDumLG3H2Bp"),
        )
        .await;
        let account = accounts.get_account(&id).await.unwrap().unwrap();
        crate::support::set_connection_options(
            &accounts,
            &account,
            json!({"image_input_enabled":enabled,"omit_images_when_disabled":omit}),
        )
        .await;
        let provider = provider_bps::initialize(
            provider_bps::BpsConfig {
                base_url: server.uri(),
                image_input_enabled: !enabled,
                omit_images_when_disabled: !omit,
                ..Default::default()
            },
            crate::support::ports_with(accounts),
        )
        .unwrap()
        .core_provider();
        let models = provider.query_model_capabilities().await.unwrap();
        let matched = models[0].capabilities().match_requirements(
            &CapabilityRequirements::new(OperationKind::Generate).require(Feature::Vision),
        );
        if enabled {
            assert!(matched.unwrap().is_empty());
        } else if omit {
            assert!(matched.unwrap().contains(&Feature::Vision));
        } else {
            assert!(matched.is_none());
        }
    }
}

#[tokio::test]
async fn catalog_exposes_only_permitted_models_and_their_reasoning_choices() {
    let server = MockServer::start().await;
    let accounts = Arc::new(crate::support::MemoryProviderAccountStore::default());
    let id = "acct_catalog-permissions";
    add_account(
        &accounts,
        id,
        &crate::credential::token("app_fnr0pYvVwwFDocDumLG3H2Bp"),
    )
    .await;
    Mock::given(method("GET"))
        .and(path("/basispoints/api/responses/access"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "allowed":true, "model_catalog": {
                "restricted_models":["restricted"],
                "models":[
                    {"id":"visible","efforts":[{"value":"high"},{"value":"high"},{"value":"invalid"}]},
                    {"id":"restricted"}, {"id":"unavailable","available":false},
                    {"id":"disabled","enabled":false}, {"id":"hidden","model_picker_enabled":false},
                    {"id":"policy","policy":{"state":"unconfigured"}}
                ]
            }
        })))
        .mount(&server).await;
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
    .core_provider();
    let models = provider.query_model_capabilities().await.unwrap();
    assert_eq!(models.len(), 1);
    assert_eq!(models[0].upstream_model().as_str(), "visible");
    assert!(
        models[0]
            .catalog_accounts()
            .unwrap()
            .contains(&ProviderAccountId::new(id).unwrap())
    );
    let presentation = models[0].presentation().unwrap();
    assert_eq!(presentation.default_reasoning_effort(), Some("high"));
    assert_eq!(presentation.supported_reasoning_efforts(), ["high"]);
}

#[tokio::test]
async fn a_late_older_response_cannot_restore_permissions_withdrawn_by_a_newer_query() {
    use std::{
        io::{Read, Write},
        net::TcpListener,
        sync::mpsc,
        time::Duration,
    };

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let uri = format!("http://{}", listener.local_addr().unwrap());
    let first_started = Arc::new(tokio::sync::Notify::new());
    let observed = first_started.clone();
    let (release, wait_for_release) = mpsc::channel();
    // 显式闸门保证旧响应在新观测提交之后才返回，不依赖机器负载或固定 sleep
    let server = std::thread::spawn(move || {
        let mut responders = Vec::new();
        let mut wait_for_release = Some(wait_for_release);
        for index in 0..2 {
            let (mut socket, _) = listener.accept().unwrap();
            let gate = wait_for_release.take();
            let observed = observed.clone();
            responders.push(std::thread::spawn(move || {
                socket.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
                let mut request = Vec::new();
                while !request.ends_with(b"\r\n\r\n") {
                    let mut byte = [0];
                    assert_eq!(socket.read(&mut byte).unwrap(), 1);
                    request.push(byte[0]);
                }
                let body = if index == 0 {
                    observed.notify_one();
                    gate.unwrap().recv_timeout(Duration::from_secs(10)).unwrap();
                    json!({"allowed":true,"model_catalog":{"models":[{"id":"revoked-model"}]}})
                } else {
                    json!({"allowed":false})
                }.to_string();
                write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body).unwrap();
            }));
        }
        for responder in responders {
            responder.join().unwrap();
        }
    });
    let accounts = Arc::new(crate::support::MemoryProviderAccountStore::default());
    add_account(
        &accounts,
        "acct_catalog-concurrent",
        &crate::credential::token("app_fnr0pYvVwwFDocDumLG3H2Bp"),
    )
    .await;
    let provider = provider_bps::initialize(
        provider_bps::BpsConfig {
            base_url: uri.clone(),
            auth_base_url: uri.clone(),
            quota_base_url: uri,
            ..Default::default()
        },
        crate::support::ports_with(accounts),
    )
    .unwrap()
    .core_provider();
    let older = {
        let provider = provider.clone();
        tokio::spawn(async move { provider.query_model_capabilities().await })
    };
    tokio::time::timeout(Duration::from_secs(5), first_started.notified())
        .await
        .expect("the older query must reach the upstream before starting the newer query");
    assert!(
        provider
            .query_model_capabilities()
            .await
            .unwrap()
            .is_empty()
    );
    release.send(()).unwrap();
    assert!(
        older.await.unwrap().unwrap().is_empty(),
        "the late query must return the already published newer observation"
    );
    assert!(
        provider
            .query_model_capabilities()
            .await
            .unwrap()
            .is_empty()
    );
    server.join().unwrap();
}
