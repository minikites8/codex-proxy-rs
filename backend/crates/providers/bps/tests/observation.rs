//! 验证 BPS 原始响应计时边界、首次冻结与失败保留

use futures::StreamExt;
use gateway_core::event::ProviderResponseTimings;
use serde_json::json;
use std::time::Duration;
use wiremock::ResponseTemplate;

#[tokio::test]
async fn output_phases_freeze_and_failure_preserves_observation() {
    let events = [
        json!({"type":"response.created","response":{"id":"resp_timing"}}),
        json!({"type":"response.output_text.delta","delta":""}),
        json!({"type":"response.reasoning_summary_text.delta","delta":"reason"}),
        json!({"type":"response.output_text.delta","delta":"text"}),
        json!({"type":"response.output_text.delta","delta":"more"}),
        json!({"type":"response.failed","response":{"id":"resp_timing","status":"failed","error":{"code":"request_rejected"}}}),
    ];
    let body = events
        .iter()
        .map(|event| format!("data: {event}\n\n"))
        .collect::<String>();
    let server =
        crate::stream::server(ResponseTemplate::new(200).set_body_raw(body, "text/event-stream"))
            .await;
    let mut stream = crate::stream::stream(&server, "openai").await;
    let first = stream
        .next()
        .await
        .unwrap()
        .unwrap()
        .response_observation()
        .unwrap()
        .timings();
    assert!(first.first_token_ms.is_some());
    assert_eq!(first.first_text_ms, None);
    assert_eq!(first.first_reasoning_ms, None);
    tokio::time::sleep(Duration::from_millis(10)).await;
    let empty = stream
        .next()
        .await
        .unwrap()
        .unwrap()
        .response_observation()
        .unwrap()
        .timings();
    assert_eq!(empty, first);
    let reasoning = stream
        .next()
        .await
        .unwrap()
        .unwrap()
        .response_observation()
        .unwrap()
        .timings();
    assert!(reasoning.first_reasoning_ms.is_some());
    assert!(reasoning.first_reasoning_ms > first.first_token_ms);
    assert_eq!(reasoning.first_text_ms, None);
    tokio::time::sleep(Duration::from_millis(10)).await;
    let text = stream
        .next()
        .await
        .unwrap()
        .unwrap()
        .response_observation()
        .unwrap()
        .timings();
    assert!(text.first_text_ms.is_some());
    assert!(text.first_text_ms > reasoning.first_reasoning_ms);
    assert_eq!(text.first_reasoning_ms, reasoning.first_reasoning_ms);
    assert_eq!(text.first_token_ms, first.first_token_ms);
    tokio::time::sleep(Duration::from_millis(10)).await;
    let next = stream
        .next()
        .await
        .unwrap()
        .unwrap()
        .response_observation()
        .unwrap()
        .timings();
    assert_eq!(next, text);
    let mut error = stream.next().await.unwrap().unwrap_err();
    let facts = error.take_atomic_client_events();
    assert_eq!(facts[0].response_observation().unwrap().timings(), text);
}

#[tokio::test]
async fn withheld_tool_arguments_report_timing_before_terminal_delivery() {
    let body = format!(
        "data: {}\n\ndata: {}\n\n",
        json!({"type":"response.function_call_arguments.delta","delta":"pending"}),
        json!({"type":"response.completed","response":{"id":"resp_tool_timing","status":"completed","output":[]}})
    );
    let server =
        crate::stream::server(ResponseTemplate::new(200).set_body_raw(body, "text/event-stream"))
            .await;
    let mut stream = crate::stream::stream(&server, "openai").await;
    let event = stream.next().await.unwrap().unwrap();
    assert!(event.wire_event().is_none());
    let timings = event.response_observation().unwrap().timings();
    assert!(timings.first_token_ms.is_some());
    assert_eq!(timings.first_text_ms, None);
    assert_eq!(timings.first_reasoning_ms, None);
    let terminal = stream.next().await.unwrap().unwrap();
    assert_eq!(
        terminal.wire_event().unwrap().data()["type"],
        "response.completed"
    );
    assert_eq!(terminal.response_observation().unwrap().timings(), timings);
    assert_ne!(timings, ProviderResponseTimings::default());
}
