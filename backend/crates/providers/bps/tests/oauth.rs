//! OAuth 状态、所有者与授权码单次交换边界
use gateway_admin::model::{
    MutationActor, MutationContext,
    provider_credentials::{
        AuthorizationMutationTarget, AuthorizationOwnerBinding, CompleteAuthorization,
        PendingAuthorizationMutation,
    },
};
use gateway_core::routing::ProviderKind;
use serde_json::json;
use url::Url;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path, query_param},
};
fn context(owner: &str) -> MutationContext {
    MutationContext {
        actor: MutationActor::AdminSession {
            admin_user_id: owner.into(),
        },
        request_id: "test-request".into(),
    }
}
#[tokio::test]
async fn binds_pkce_state_and_owner_before_single_code_exchange() {
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
    let owner = context("owner");
    let started = provider
        .start_authorization(PendingAuthorizationMutation::new(
            ProviderKind::new("bps").unwrap(),
            AuthorizationMutationTarget::Create {
                name: "test BPS".into(),
            },
            AuthorizationOwnerBinding::from_context(&owner),
        ))
        .await
        .unwrap();
    let authorization = Url::parse(&started.authorization_url).unwrap();
    let pairs = authorization
        .query_pairs()
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(pairs["code_challenge_method"], "S256");
    assert!(!pairs["code_challenge"].is_empty());
    let callback = format!(
        "{}?state={}&code=once",
        pairs["redirect_uri"], pairs["state"]
    );
    assert!(
        provider
            .complete_authorization(CompleteAuthorization {
                settings: None,
                context: context("other"),
                flow_id: started.flow_id.clone(),
                callback_url: callback.clone()
            })
            .await
            .is_err()
    );
    let bad = callback.replace(pairs["state"].as_ref(), "wrong-state");
    assert!(
        provider
            .complete_authorization(CompleteAuthorization {
                settings: None,
                context: owner.clone(),
                flow_id: started.flow_id.clone(),
                callback_url: bad
            })
            .await
            .is_err()
    );
    Mock::given(method("POST")).and(path("/oauth/token"))
        .and(query_param("unified", "true")).respond_with(ResponseTemplate::new(200).set_body_json(json!({"access_token":crate::credential::token("app_fnr0pYvVwwFDocDumLG3H2Bp"),"refresh_token":"test-refresh"}))).expect(1).mount(&server).await;
    Mock::given(method("GET"))
        .and(path("/basispoints/api/responses/access"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"allowed":true,"model_catalog":{"models":[]}})),
        )
        .mount(&server)
        .await;
    let command = CompleteAuthorization {
        settings: None,
        context: owner,
        flow_id: started.flow_id,
        callback_url: callback,
    };
    let _prepared = provider
        .complete_authorization(command.clone())
        .await
        .unwrap();
    assert!(provider.complete_authorization(command).await.is_err());
    let requests = server.received_requests().await.unwrap();
    let request = requests
        .iter()
        .find(|request| request.method.as_str() == "POST")
        .unwrap();
    assert_eq!(
        request.headers.get("content-type").unwrap(),
        "application/x-www-form-urlencoded"
    );
    let body = url::form_urlencoded::parse(&request.body)
        .into_owned()
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(body["client_id"], "app_fnr0pYvVwwFDocDumLG3H2Bp");
    assert_eq!(body["grant_type"], "authorization_code");
    use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
    use sha2::{Digest as _, Sha256};
    assert_eq!(
        URL_SAFE_NO_PAD.encode(Sha256::digest(body["code_verifier"].as_bytes())),
        pairs["code_challenge"]
    );
}
