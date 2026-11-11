//! BPS 仅使用明确配置的单价，不从同名 Codex 模型推断订阅费用

use gateway_core::metering::{
    CalculatedCostAmounts, CalculatedCostBreakdown, CalculatedCostRates, CurrencyCode, Decimal,
    ModelPriceOverride, Money, Usage,
};

pub(crate) fn calculate(
    usage: &Usage,
    pricing: Option<&ModelPriceOverride>,
) -> Option<CalculatedCostBreakdown> {
    let pricing = pricing?;
    pricing.validate().ok()?;
    let rates = pricing.bands.get("standard")?;
    // 缺失计量不能当作免费；图片单独计价的语义尚未接入
    if usage.image_input_tokens.is_some_and(|value| value > 0)
        || usage.image_output_tokens.is_some_and(|value| value > 0)
    {
        return None;
    }
    let cached = usage.cached_input_tokens?;
    let written = usage.cache_write_input_tokens?;
    let input = usage
        .input_tokens?
        .checked_sub(cached)?
        .checked_sub(written)?;
    let output = usage.output_tokens?;
    let input_ticks = u128::from(input).checked_mul(rates.input.ticks_per_token())?;
    let output_ticks = u128::from(output).checked_mul(rates.output.ticks_per_token())?;
    let cached_ticks = u128::from(cached).checked_mul(rates.cache_read.ticks_per_token())?;
    let written_ticks = u128::from(written).checked_mul(rates.cache_write.ticks_per_token())?;
    let total = input_ticks
        .checked_add(output_ticks)?
        .checked_add(cached_ticks)?
        .checked_add(written_ticks)?;
    CalculatedCostBreakdown::new(
        CalculatedCostAmounts::new(
            usd(input_ticks)?,
            usd(output_ticks)?,
            usd(cached_ticks)?,
            usd(written_ticks)?,
            usd(total)?,
            usd(total)?,
        ),
        CalculatedCostRates::new(
            usd(rates.input.ticks_per_token().checked_mul(1_000_000)?)?,
            usd(rates.output.ticks_per_token().checked_mul(1_000_000)?)?,
            usd(rates.cache_read.ticks_per_token().checked_mul(1_000_000)?)?,
            usd(rates.cache_write.ticks_per_token().checked_mul(1_000_000)?)?,
        ),
        None,
        100,
    )
    .with_custom_multiplier(pricing.multiplier_bps)
}

fn usd(ticks: u128) -> Option<Money> {
    Some(Money::new(
        Decimal::from_scaled(ticks).ok()?,
        CurrencyCode::new("USD").ok()?,
    ))
}
