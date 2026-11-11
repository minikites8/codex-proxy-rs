//! BPS 文本即时交付与失败终态不执行工具

use gateway_protocol::bps::{BpsRequest, BpsResponse, RequestIdentity};
use serde_json::json;

#[test]
fn text_is_delivered_incrementally_and_failed_terminal_is_preserved() {
    let request = BpsRequest::prepare(
        &json!({"input":"hello"}),
        "model",
        RequestIdentity {
            task_id: "task".into(),
            turn_id: "turn".into(),
            prompt_cache_key: None,
        },
        &[],
    )
    .unwrap();
    let mut stream = BpsResponse::new(request);
    let delta = json!({"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":"hello"});
    assert_eq!(stream.event(delta.clone()).unwrap(), vec![delta]);
    let failed = json!({"type":"response.failed","response":{"id":"resp_a","status":"failed","error":{"code":"rate_limit_exceeded"}}});
    assert_eq!(stream.event(failed.clone()).unwrap(), vec![failed]);
    assert!(stream.is_terminal());
    assert!(stream.replays().is_empty());
}

#[test]
fn truncated_stream_is_not_a_successful_response() {
    let request = BpsRequest::prepare(
        &json!({"input":"hello"}),
        "model",
        RequestIdentity {
            task_id: "task".into(),
            turn_id: "turn".into(),
            prompt_cache_key: None,
        },
        &[],
    )
    .unwrap();
    let mut stream = BpsResponse::new(request);
    stream
        .event(json!({"type":"response.output_text.delta","delta":"partial"}))
        .unwrap();
    assert!(stream.finish().is_err());
}

#[test]
fn cancelled_terminal_retains_actual_usage_without_committing_tools() {
    let request = BpsRequest::prepare(
        &json!({"input":"hello"}),
        "model",
        RequestIdentity {
            task_id: "task".into(),
            turn_id: "turn".into(),
            prompt_cache_key: None,
        },
        &[],
    )
    .unwrap();
    let mut stream = BpsResponse::new(request);
    let terminal = json!({"type":"response.cancelled","response":{"id":"resp_a","status":"cancelled","usage":{"input_tokens":7,"output_tokens":2}}});
    assert_eq!(stream.event(terminal.clone()).unwrap(), vec![terminal]);
    assert!(stream.finish().is_ok());
    assert!(stream.replays().is_empty());
}

#[test]
fn unsuccessful_terminal_keeps_error_and_usage_but_withholds_native_tools() {
    for kind in [
        "response.incomplete",
        "response.failed",
        "response.cancelled",
    ] {
        let request = BpsRequest::prepare(
            &json!({"input":"hello"}),
            "model",
            RequestIdentity {
                task_id: "t".into(),
                turn_id: "r".into(),
                prompt_cache_key: None,
            },
            &[],
        )
        .unwrap();
        let mut stream = BpsResponse::new(request);
        let terminal = json!({"type":kind,"response":{"id":"resp_unsafe","status":"incomplete","error":{"code":"actual_error","message":"real upstream message"},"usage":{"output_tokens":4},"output":[{"type":"function_call","name":"run_officejs","call_id":"call_unsafe","arguments":"{}"},{"type":"message","content":[]}]}});
        let result = stream.event(terminal.clone()).unwrap();
        assert_eq!(
            result[0]["response"]["error"],
            terminal["response"]["error"]
        );
        assert_eq!(
            result[0]["response"]["usage"],
            terminal["response"]["usage"]
        );
        assert_eq!(result[0]["response"]["output"].as_array().unwrap().len(), 1);
        assert!(stream.replays().is_empty());
    }
}
