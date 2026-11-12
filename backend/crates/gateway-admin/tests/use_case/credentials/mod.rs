//! 按 Provider 组织凭据导入、授权与提交边界测试

mod openai;
mod peer;
mod xai;

#[tokio::test]
async fn account_rotation_dispatches_by_persisted_provider() {
    use super::accounts::{
        FakeAccountStore, FakeProviderAdmin, context, document, events, recorded,
    };
    use gateway_admin::model::provider_credentials::{CredentialMutation, RotateCredential};
    use gateway_core::account::ProviderAccountId;

    for kind in ["openai", "bps"] {
        let events = events();
        let store = FakeAccountStore::new(kind, events.clone());
        let services = super::AdminHarness::new()
            .provider(FakeProviderAdmin::new(kind, events.clone()))
            .accounts(store.clone())
            .build()
            .await;
        let id = ProviderAccountId::new("acct_test").unwrap();
        let result = services
            .credentials()
            .for_account(&id)
            .await
            .unwrap()
            .rotate(RotateCredential {
                mutation: CredentialMutation {
                    context: context("account-transport"),
                    account_id: id.clone(),
                },
                settings: None,
                provider_material: document(),
            })
            .await
            .unwrap();
        assert_eq!(result.account_id, id);
        assert!(recorded(&events).contains(&"provider.prepare_rotation"));
        assert_eq!(store.audit_requests(), ["account-transport"]);
        assert!(
            services
                .credentials()
                .for_account(&ProviderAccountId::new("acct_missing").unwrap())
                .await
                .is_err()
        );
    }
}
