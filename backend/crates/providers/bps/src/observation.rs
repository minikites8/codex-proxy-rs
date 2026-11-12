//! BPS 原始响应边界计时，工具暂存不改变上游首输出事实

use gateway_core::{
    event::{ProviderResponseObservation, ProviderResponseTimings},
    upstream::UpstreamTransport,
};
use serde_json::Value;
use std::time::Instant;

pub(crate) struct Observation {
    started_at: Instant,
    response: ProviderResponseObservation,
    timings: ProviderResponseTimings,
}

impl Observation {
    pub(crate) fn new(started_at: Instant, transport: UpstreamTransport, status: u16) -> Self {
        let timings = ProviderResponseTimings {
            headers_ms: Some(elapsed_ms(started_at)),
            ..ProviderResponseTimings::default()
        };
        Self {
            started_at,
            response: ProviderResponseObservation::new(transport).with_status_code(status),
            timings,
        }
    }

    pub(crate) fn observe(&mut self, event: &Value) -> bool {
        let previous = self.timings;
        let elapsed = elapsed_ms(self.started_at);
        self.timings.first_event_ms.get_or_insert(elapsed);
        let kind = event
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default();
        // 首字沿用 Responses 数据事件边界，独立错误与传输控制帧不计入
        if kind.starts_with("response.") {
            self.timings.first_token_ms.get_or_insert(elapsed);
        }
        let nonempty = event
            .get("delta")
            .and_then(Value::as_str)
            .is_some_and(|text| !text.is_empty());
        if nonempty {
            match kind {
                "response.output_text.delta" => {
                    self.timings.first_text_ms.get_or_insert(elapsed);
                }
                "response.reasoning_text.delta" | "response.reasoning_summary_text.delta" => {
                    self.timings.first_reasoning_ms.get_or_insert(elapsed);
                }
                _ => {}
            }
        }
        self.timings != previous
    }

    pub(crate) fn snapshot(&self) -> ProviderResponseObservation {
        self.response.clone().with_timings(self.timings)
    }
}

fn elapsed_ms(started_at: Instant) -> u64 {
    u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX)
}
