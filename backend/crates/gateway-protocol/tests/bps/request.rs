//! BPS 请求保留模型、历史与显式能力约束

use gateway_protocol::bps::{BpsRequest, RequestIdentity};
use serde_json::json;

#[test]
fn translates_text_and_reasoning_without_changing_model() {
    let request = BpsRequest::prepare(
        &json!({
            "model": "requested-model", "input": "hello", "instructions": "Help with code",
            "reasoning": {"effort": "xhigh"}
        }),
        "requested-model",
        RequestIdentity {
            task_id: "task".into(),
            turn_id: "turn".into(),
            prompt_cache_key: None,
        },
        &[],
    )
    .unwrap();
    assert_eq!(request.body()["model"], "requested-model");
    assert_eq!(request.body()["reasoning_effort"], "xhigh");
    assert_eq!(
        request.body()["input"][0]["content"][0]["text"],
        "Help with code"
    );
    assert_eq!(
        request.body()["input"].as_array().unwrap().last().unwrap()["content"][0]["text"],
        "hello"
    );
}

#[test]
fn rejects_capabilities_that_cannot_be_preserved() {
    for patch in [
        json!({"previous_response_id":"resp_other"}),
        json!({"tool_choice":"required"}),
        json!({"text":{"format":{"type":"json_schema","schema":{"type":"object"}}}}),
        json!({"tools":[{"type":"image_generation"}]}),
        json!({"input":[{"role":"user","content":[{"type":"encrypted_content","encrypted_content":"secret"}]}]}),
    ] {
        let mut source = json!({"input":"hello"});
        source
            .as_object_mut()
            .unwrap()
            .extend(patch.as_object().unwrap().clone());
        let result = BpsRequest::prepare(
            &source,
            "model",
            RequestIdentity {
                task_id: "t".into(),
                turn_id: "r".into(),
                prompt_cache_key: None,
            },
            &[],
        );
        let error = result.expect_err("unsupported input must fail before sending");
        assert!(!error.to_string().contains("secret"));
    }
}
