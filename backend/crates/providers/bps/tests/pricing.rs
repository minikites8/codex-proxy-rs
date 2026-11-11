//! 验证显式 BPS 单价与未知计量的计费边界

use gateway_admin::ports::provider::ProviderUsagePricingInput;
use gateway_core::metering::{ModelPriceOverride, Usage};
use serde_json::json;

#[test]
fn explicit_prices_preserve_cache_costs_and_unknown_usage() {
    let provider = provider_bps::initialize(Default::default(), crate::support::ports())
        .unwrap()
        .admin_provider();
    let mut input = ProviderUsagePricingInput {
        upstream_model_id: "gpt-test".into(),
        service_tier: None,
        usage: Usage {
            input_tokens: Some(1_000_000),
            output_tokens: Some(100_000),
            cached_input_tokens: Some(200_000),
            cache_write_input_tokens: Some(100_000),
            ..Usage::new()
        },
    };
    let pricing: ModelPriceOverride = serde_json::from_value(json!({
        "multiplierBps": 15000,
        "bands": {"standard": {"input":"2", "output":"10", "cacheRead":"0.5", "cacheWrite":"3"}}
    }))
    .unwrap();
    assert!(
        provider
            .evaluate_usage_pricing(&input, None)
            .unwrap()
            .is_none()
    );
    let cost = provider
        .evaluate_usage_pricing(&input, Some(&pricing))
        .unwrap()
        .unwrap();
    assert_eq!(cost.total_amount().amount().canonical(), "4.2");
    assert_eq!(cost.input_amount().amount().canonical(), "2.1");
    assert_eq!(cost.cache_read_amount().amount().canonical(), "0.15");
    assert_eq!(cost.custom_multiplier_bps(), 15000);
    input.usage.cached_input_tokens = None;
    assert!(
        provider
            .evaluate_usage_pricing(&input, Some(&pricing))
            .unwrap()
            .is_none()
    );
    input.usage.cached_input_tokens = Some(1_000_001);
    assert!(
        provider
            .evaluate_usage_pricing(&input, Some(&pricing))
            .unwrap()
            .is_none()
    );
    input.usage.cached_input_tokens = Some(0);
    input.usage.image_input_tokens = Some(1);
    assert!(
        provider
            .evaluate_usage_pricing(&input, Some(&pricing))
            .unwrap()
            .is_none()
    );
}
