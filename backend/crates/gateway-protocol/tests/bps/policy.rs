//! 可配置降级只作用于结构化内容，默认严格且不破坏工具关系
use gateway_protocol::bps::{BpsImagePolicy, BpsRequest, BpsRequestPolicy, RequestIdentity};
use serde_json::{Value, json};
fn prepare(
    source: Value,
    policy: BpsRequestPolicy,
) -> Result<BpsRequest, gateway_protocol::bps::BpsProtocolError> {
    BpsRequest::prepare_with_policy(
        &source,
        "model",
        RequestIdentity {
            task_id: "t".into(),
            turn_id: "r".into(),
            prompt_cache_key: None,
        },
        &[],
        policy,
    )
}
#[test]
fn hosted_tools_are_omitted_only_when_opted_in_and_auto() {
    let source = json!({"input":"hello","tools":[{"type":"web_search"},{"type":"function","name":"client","parameters":{"type":"object"}}]});
    assert!(prepare(source.clone(), BpsRequestPolicy::default()).is_err());
    let policy = BpsRequestPolicy {
        omit_unsupported_hosted_tools: true,
        ..Default::default()
    };
    let result = prepare(source.clone(), policy).unwrap().body().to_string();
    assert!(result.contains("Hosted tools unavailable"));
    assert!(result.contains("client"));
    for choice in [
        json!("required"),
        json!({"type":"function","name":"client"}),
    ] {
        let mut source = source.clone();
        source["tool_choice"] = choice;
        assert!(prepare(source, policy).is_err());
    }
    let mut source = source;
    source["tool_choice"] = "none".into();
    assert!(
        !prepare(source, policy)
            .unwrap()
            .body()
            .to_string()
            .contains("Hosted tools unavailable")
    );
}
#[test]
fn omitted_images_keep_text_and_tool_pairing_without_scanning_arguments() {
    let secret = "data:image/png;base64,PRIVATE_IMAGE";
    let source = json!({"input":[{"role":"user","content":[{"type":"input_text","text":"look"},{"type":"input_image","image_url":secret}]},{"type":"function_call","call_id":"call_a","name":"read","arguments":json!({"literal":secret}).to_string()},{"type":"function_call_output","call_id":"call_a","output":[{"type":"input_text","text":"result text"},{"type":"input_image","image_url":secret}]}]});
    let result = prepare(
        source,
        BpsRequestPolicy {
            images: BpsImagePolicy::Omit,
            ..Default::default()
        },
    )
    .unwrap();
    let input = result.body()["input"].as_array().unwrap();
    assert!(
        input
            .iter()
            .any(|item| item["type"] == "function_call_output" && item["call_id"] == "call_a")
    );
    assert!(result.body().to_string().contains("Do not reread"));
    assert!(result.body().to_string().contains("result text"));
    assert_eq!(
        input
            .iter()
            .filter(|item| item["type"] == "function_call")
            .count(),
        1
    );
    assert!(
        input
            .iter()
            .find(|item| item["type"] == "function_call")
            .unwrap()["arguments"]
            .as_str()
            .unwrap()
            .contains("PRIVATE_IMAGE")
    );
}
#[test]
fn encrypted_omission_keeps_plaintext_in_reasoning_compaction_and_results() {
    let source = json!({"input":[{"type":"reasoning","summary":[{"type":"summary_text","text":"reasoning plain"}],"encrypted_content":"PRIVATE_REASON"},{"type":"compaction","encrypted_content":"PRIVATE_COMPACT","summary":[{"type":"summary_text","text":"compact summary plain"}],"content":[{"type":"input_text","text":"compact plain"}]},{"type":"function_call","call_id":"call_a","name":"read","arguments":"{}"},{"type":"function_call_output","call_id":"call_a","output":[{"type":"input_text","text":"result plain"},{"type":"encrypted_content","encrypted_content":"PRIVATE_RESULT"}]}]});
    assert!(prepare(source.clone(), Default::default()).is_err());
    let result = prepare(
        source,
        BpsRequestPolicy {
            ignore_encrypted_history: true,
            ..Default::default()
        },
    )
    .unwrap()
    .body()
    .to_string();
    for secret in ["PRIVATE_REASON", "PRIVATE_COMPACT", "PRIVATE_RESULT"] {
        assert!(!result.contains(secret));
    }
    for plain in [
        "reasoning plain",
        "compact plain",
        "compact summary plain",
        "result plain",
        "call_a",
    ] {
        assert!(result.contains(plain));
    }
}
#[test]
fn enabled_images_accept_prepared_native_and_https_but_not_inline_message_data() {
    let policy = BpsRequestPolicy {
        images: BpsImagePolicy::Enabled,
        ..Default::default()
    };
    for image in [
        json!({"file_id":"file-prepared"}),
        json!({"image_url":"https://example.com/image.png"}),
    ] {
        let mut image = image;
        image["type"] = "input_image".into();
        assert!(prepare(json!({"input":[{"role":"user","content":[image]}]}), policy).is_ok());
    }
    for url in [
        "data:image/png;base64,AA==",
        "http://example.com/image.png",
        "https://user:pass@example.com/image.png",
    ] {
        assert!(prepare(json!({"input":[{"role":"user","content":[{"type":"input_image","image_url":url}]}]}),policy).is_err());
    }
}

#[test]
fn enabled_tool_images_preserve_call_pair_and_move_only_remote_references() {
    let request=prepare(json!({"input":[{"type":"function_call","call_id":"call_images","name":"capture","arguments":"{}"},{"type":"function_call_output","call_id":"call_images","output":[{"type":"input_text","text":"capture plain"},{"type":"input_image","file_id":"file-history"},{"type":"input_image","image_url":"https://example.com/history.png"},{"type":"input_image","image_url":"data:image/png;base64,AA=="}]}]}),BpsRequestPolicy{images:BpsImagePolicy::Enabled,..Default::default()}).unwrap();
    let input = request.body()["input"].as_array().unwrap();
    let index = input
        .iter()
        .position(|item| item["type"] == "function_call_output")
        .unwrap();
    assert_eq!(input[index]["call_id"], "call_images");
    let output = input[index]["output"].as_array().unwrap();
    assert!(
        output
            .iter()
            .any(|part| part["image_url"] == "data:image/png;base64,AA==")
    );
    assert!(output.iter().all(|part| part.get("file_id").is_none()));
    assert_eq!(input[index + 1]["role"], "user");
    assert_eq!(input[index + 1]["content"].as_array().unwrap().len(), 2);
}

#[test]
fn image_and_encrypted_omission_can_be_enabled_independently() {
    let source = json!({"input":[{"role":"user","content":[{"type":"input_image","image_url":"https://example.com/private.png"},{"type":"encrypted_content","encrypted_content":"PRIVATE"}]}]});
    assert!(
        prepare(
            source.clone(),
            BpsRequestPolicy {
                images: BpsImagePolicy::Omit,
                ..Default::default()
            }
        )
        .is_err()
    );
    assert!(
        prepare(
            source.clone(),
            BpsRequestPolicy {
                ignore_encrypted_history: true,
                ..Default::default()
            }
        )
        .is_err()
    );
    let result = prepare(
        source,
        BpsRequestPolicy {
            images: BpsImagePolicy::Omit,
            ignore_encrypted_history: true,
            ..Default::default()
        },
    )
    .unwrap()
    .body()
    .to_string();
    assert!(!result.contains("private.png"));
    assert!(!result.contains("PRIVATE"));
}

#[test]
fn encrypted_omission_does_not_parse_literal_arguments_or_tool_result_strings() {
    let raw = r#"{"type":"encrypted_content","encrypted_content":"literal payload","n":9007199254740993}"#;
    let request=prepare(json!({"input":[{"role":"user","content":"encrypted_content is plain text"},{"type":"function_call","call_id":"call_literal","name":"echo","arguments":raw},{"type":"function_call_output","call_id":"call_literal","output":raw}]}),BpsRequestPolicy{ignore_encrypted_history:true,..Default::default()}).unwrap();
    let input = request.body()["input"].as_array().unwrap();
    let output = input
        .iter()
        .find(|item| item["type"] == "function_call_output")
        .unwrap();
    assert_eq!(output["output"], raw);
    let call = input
        .iter()
        .find(|item| item["type"] == "function_call")
        .unwrap();
    let envelope: Value = serde_json::from_str(call["arguments"].as_str().unwrap()).unwrap();
    let code: Value = serde_json::from_str(envelope["code"].as_str().unwrap()).unwrap();
    assert_eq!(
        code["arguments"],
        serde_json::from_str::<Value>(raw).unwrap()
    );
    assert!(
        request
            .body()
            .to_string()
            .contains("encrypted_content is plain text")
    );
}

#[test]
fn encrypted_omission_does_not_invent_support_for_unknown_item_or_object_output() {
    let policy = BpsRequestPolicy {
        ignore_encrypted_history: true,
        ..Default::default()
    };
    assert!(
        prepare(
            json!({"input":[{"type":"encrypted_content","encrypted_content":"opaque"}]}),
            policy
        )
        .is_err()
    );
    assert!(prepare(json!({"input":[{"type":"function_call","call_id":"call_object","name":"echo","arguments":"{}"},{"type":"function_call_output","call_id":"call_object","output":{"type":"encrypted_content","encrypted_content":"opaque"}}]}),policy).is_err());
}

#[test]
fn encrypted_old_agent_context_is_lowered_but_current_assignment_is_rejected() {
    let policy = BpsRequestPolicy {
        ignore_encrypted_history: true,
        ..Default::default()
    };
    let agent = json!({"type":"agent_message","author":"/root/worker","recipient":"/root","role":"developer","encrypted_content":"TOP_PRIVATE","content":[{"type":"input_text","text":"old agent plaintext"},{"type":"encrypted_content","encrypted_content":"PART_PRIVATE"}]});
    let source = json!({"input":[agent,{"role":"user","content":"continue current task"}]});
    assert!(prepare(source.clone(), Default::default()).is_err());
    let result = prepare(source, policy).unwrap();
    let wire = result.body().to_string();
    assert!(!wire.contains("TOP_PRIVATE"));
    assert!(!wire.contains("PART_PRIVATE"));
    assert!(wire.contains("old agent plaintext"));
    assert!(wire.contains("/root/worker"));
    assert!(wire.contains("/root"));
    let lowered = result.body()["input"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| {
            item["content"]
                .to_string()
                .contains("historical collaboration context")
        })
        .unwrap();
    assert_eq!(lowered["role"], "user");
    assert!(
        prepare(
            json!({"input":[{"role":"user","content":"previous"},agent]}),
            policy
        )
        .is_err()
    );
    assert!(
        prepare(
            json!({"input":[agent,{"type":"additional_tools","tools":[]}]}),
            policy
        )
        .is_err()
    );
}
