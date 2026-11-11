//! Excel 凭据导入与 Codex 客户端隔离
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use gateway_admin::model::provider_credentials::{PrepareCredentialImport, ProviderDocument};
use gateway_core::account::OpaqueProviderData;
use serde_json::{Value, json};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

pub fn token(client: &str) -> String {
    format!("e30.{}.signature",URL_SAFE_NO_PAD.encode(json!({"client_id":client,"sub":"user-test","exp":4102444800i64,"https://api.openai.com/auth":{"chatgpt_account_id":"workspace-test","chatgpt_user_id":"user-test"}}).to_string()))
}
pub fn document(value: Value) -> ProviderDocument {
    ProviderDocument::new(OpaqueProviderData::new(value.as_object().unwrap().clone()))
}
#[tokio::test]
async fn imports_excel_session_and_rejects_claims_from_codex_client() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/basispoints/api/responses/access"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"allowed":true,"model_catalog":{"models":[]}})),
        )
        .expect(1)
        .mount(&server)
        .await;
    let config = provider_bps::BpsConfig {
        base_url: server.uri(),
        auth_base_url: server.uri(),
        quota_base_url: server.uri(),
        ..Default::default()
    };
    let provider = provider_bps::initialize(config, crate::support::ports())
        .unwrap()
        .admin_provider();
    let excel = "app_fnr0pYvVwwFDocDumLG3H2Bp";
    let prepared=provider.prepare_import(PrepareCredentialImport {default_outbound_proxy:None,document:document(json!({"client_id":excel,"access_token":token(excel),"refresh_token":"test-refresh"}))}).await.unwrap();
    assert_eq!(prepared.credentials.len(), 1);
    assert_eq!(
        prepared.credentials[0].upstream_account_id.as_deref(),
        Some("workspace-test")
    );
    assert!(
        provider
            .prepare_import(PrepareCredentialImport {
                default_outbound_proxy: None,
                document: document(
                    json!({"client_id":excel,"access_token":token("app_EMoamEEZ73f0CkXaXp7hrann")})
                )
            })
            .await
            .is_err()
    );
}

#[tokio::test]
async fn rejects_declared_account_that_conflicts_with_token_identity() {
    let provider =
        provider_bps::initialize(provider_bps::BpsConfig::default(), crate::support::ports())
            .unwrap()
            .admin_provider();
    let excel = "app_fnr0pYvVwwFDocDumLG3H2Bp";
    assert!(provider.prepare_import(PrepareCredentialImport {
        default_outbound_proxy: None,
        document: document(json!({"client_id":excel,"access_token":token(excel),"chatgpt_account_id":"different-workspace"})),
    }).await.is_err());
}

#[tokio::test]
async fn expired_import_does_not_consume_refresh_token() {
    let server = MockServer::start().await;
    let provider = provider_bps::initialize(
        provider_bps::BpsConfig {
            base_url: server.uri(),
            auth_base_url: server.uri(),
            quota_base_url: server.uri(),
            ..Default::default()
        },
        crate::support::ports(),
    )
    .unwrap()
    .admin_provider();
    let excel = "app_fnr0pYvVwwFDocDumLG3H2Bp";
    let access = format!("e30.{}.signature", URL_SAFE_NO_PAD.encode(json!({"client_id":excel,"sub":"user-test","exp":1,"https://api.openai.com/auth":{"chatgpt_account_id":"workspace-test","chatgpt_user_id":"user-test"}}).to_string()));
    let error = provider
        .prepare_import(PrepareCredentialImport {
            default_outbound_proxy: None,
            document: document(json!({"access_token":access,"refresh_token":"untouched-refresh"})),
        })
        .await
        .unwrap_err();
    assert!(error.public_message().unwrap().contains("过期"));
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn requires_access_token_client_expiry_and_matching_identity_token_audience() {
    let provider =
        provider_bps::initialize(provider_bps::BpsConfig::default(), crate::support::ports())
            .unwrap()
            .admin_provider();
    let excel = "app_fnr0pYvVwwFDocDumLG3H2Bp";
    let base = json!({"client_id":excel,"sub":"user-test","exp":4102444800i64,"https://api.openai.com/auth":{"chatgpt_account_id":"workspace-test"}});
    let encode = |claims: Value| {
        format!(
            "e30.{}.signature",
            URL_SAFE_NO_PAD.encode(claims.to_string())
        )
    };
    for missing in ["client_id", "exp", "https://api.openai.com/auth"] {
        let mut claims = base.clone();
        claims.as_object_mut().unwrap().remove(missing);
        assert!(
            provider
                .prepare_import(PrepareCredentialImport {
                    default_outbound_proxy: None,
                    document: document(json!({"client_id":excel,"access_token":encode(claims)}))
                })
                .await
                .is_err(),
            "missing {missing}"
        );
    }
    assert!(provider.prepare_import(PrepareCredentialImport { default_outbound_proxy: None, document: document(json!({"client_id":excel,"access_token":encode(base),"id_token":encode(json!({"aud":["codex-client"]}))})) }).await.is_err());
}

#[tokio::test]
async fn imported_plan_uses_explicit_matching_token_facts() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/basispoints/api/responses/access"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"allowed":true,"model_catalog":{"models":[]}})),
        )
        .mount(&server)
        .await;
    let provider = provider_bps::initialize(
        provider_bps::BpsConfig {
            base_url: server.uri(),
            auth_base_url: server.uri(),
            quota_base_url: server.uri(),
            ..Default::default()
        },
        crate::support::ports(),
    )
    .unwrap()
    .admin_provider();
    let encode = |claims: Value| {
        format!(
            "e30.{}.signature",
            URL_SAFE_NO_PAD.encode(claims.to_string())
        )
    };
    let excel = "app_fnr0pYvVwwFDocDumLG3H2Bp";
    for (access_plan, identity_plan, identity_workspace, expected) in [
        (Some("pro"), Some("plus"), "workspace-test", Some("pro")),
        (None, Some("business"), "workspace-test", Some("business")),
        (Some("unknown"), Some("pro"), "different-workspace", None),
        (Some(" "), None, "workspace-test", None),
        (
            Some("future_plan"),
            None,
            "workspace-test",
            Some("future_plan"),
        ),
    ] {
        let access = encode(
            json!({"client_id":excel,"sub":"user-test","exp":4102444800i64,"https://api.openai.com/auth":{"chatgpt_account_id":"workspace-test","chatgpt_user_id":"user-test","chatgpt_plan_type":access_plan}}),
        );
        let identity = encode(
            json!({"aud":[excel],"https://api.openai.com/auth":{"chatgpt_account_id":identity_workspace,"chatgpt_user_id":"user-test","chatgpt_plan_type":identity_plan}}),
        );
        let prepared=provider.prepare_import(PrepareCredentialImport{default_outbound_proxy:None,document:document(json!({"access_token":access,"id_token":identity,"plan_type":"untrusted-declared-plan"}))}).await.unwrap();
        assert_eq!(prepared.credentials[0].plan_type.as_deref(), expected);
    }
}
