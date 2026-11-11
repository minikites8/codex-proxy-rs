//! BPS 工具信封保留参数与调用身份，并拒绝未声明目标

use gateway_protocol::bps::{BpsRequest, BpsResponse, RequestIdentity};
use serde_json::{Value, json};

fn request() -> BpsRequest {
    BpsRequest::prepare(&json!({"input":"test", "tools":[{"type":"function","name":"shell","parameters":{"type":"object"}}]}),
        "model", RequestIdentity {task_id:"task".into(),turn_id:"turn".into(),prompt_cache_key:None}, &[]).unwrap()
}

#[test]
fn responses_lite_catalog_items_enable_namespaced_tools_without_leaking_into_history() {
    for carrier in ["additional_tools", "tool_search_output"] {
        let source = json!({"input":[
            {"type":carrier,"role":"developer","status":"completed","execution":"server","call_id":null,
             "tools":[{"type":"namespace","name":"functions","tools":[{"type":"function","name":"read_file","parameters":{"type":"object","properties":{"path":{"type":"string"}},"required":["path"]}}]}]},
            {"role":"user","content":"Read the file"}
        ]});
        let prepared = BpsRequest::prepare(
            &source,
            "model",
            RequestIdentity {
                task_id: "t".into(),
                turn_id: "r".into(),
                prompt_cache_key: None,
            },
            &[],
        )
        .unwrap();
        assert!(
            prepared.body()["input"]
                .as_array()
                .unwrap()
                .iter()
                .all(|item| item["type"] != carrier)
        );
        assert_eq!(
            prepared.body()["input"].as_array().unwrap().last().unwrap()["content"],
            "Read the file"
        );
        let mut bridge = BpsResponse::new(prepared);
        let native = json!({"type":"function_call","call_id":"call_read","name":"run_officejs","arguments":json!({"code":json!({"name":"functions.read_file","arguments":{"path":"src/main.rs"}}).to_string()}).to_string()});
        let events = bridge.event(json!({"type":"response.completed","response":{"id":"resp_lite","output":[native]}})).unwrap();
        let call = &events.last().unwrap()["response"]["output"][0];
        assert_eq!(call["name"], "read_file");
        assert_eq!(call["namespace"], "functions");
    }
}

#[test]
fn current_catalog_wins_and_only_the_latest_selected_historical_schema_is_validated() {
    let current = json!({"type":"function","name":"read_file","parameters":{"type":"object","properties":{"path":{"type":"string"}},"required":["path"]}});
    let obsolete = json!({"type":"function","name":"read_file","parameters":{"$ref":"https://example.invalid/obsolete"}});
    for explicit in [true, false] {
        let mut source = json!({"input":[{"type":"additional_tools","tools":[obsolete.clone()]},{"type":"tool_search_output","tools":[current.clone()]},{"role":"user","content":"Read"}]});
        if explicit {
            source["tools"] = json!([current.clone()]);
            source["input"][1]["tools"] = json!([obsolete.clone()]);
        }
        for (path, accepted) in [(json!("file.rs"), true), (json!(3), false)] {
            let prepared = BpsRequest::prepare(
                &source,
                "model",
                RequestIdentity {
                    task_id: "t".into(),
                    turn_id: "r".into(),
                    prompt_cache_key: None,
                },
                &[],
            )
            .unwrap();
            let mut bridge = BpsResponse::new(prepared);
            let native = json!({"type":"function_call","call_id":"call_read","name":"run_officejs","arguments":json!({"code":json!({"name":"read_file","arguments":{"path":path}}).to_string()}).to_string()});
            assert_eq!(
                bridge
                    .event(json!({"type":"response.completed","response":{"output":[native]}}))
                    .is_ok(),
                accepted
            );
        }
    }
}

#[test]
fn history_catalogs_preserve_choice_none_and_reject_invalid_selected_declarations() {
    let identity = || RequestIdentity {
        task_id: "t".into(),
        turn_id: "r".into(),
        prompt_cache_key: None,
    };
    let mut source = json!({"tool_choice":"none","input":[{"type":"additional_tools","tools":[{"type":"function","name":"shell"}]},{"type":"tool_search_output","tools":[]},{"role":"user","content":"Read"}]});
    let prepared = BpsRequest::prepare(&source, "model", identity(), &[]).unwrap();
    assert!(
        prepared.body()["input"]
            .as_array()
            .unwrap()
            .iter()
            .all(|item| !matches!(
                item["type"].as_str(),
                Some("additional_tools" | "tool_search_output")
            ))
    );
    let mut bridge = BpsResponse::new(prepared);
    let native = json!({"type":"function_call","call_id":"call_read","name":"run_officejs","arguments":json!({"code":json!({"name":"shell","arguments":{}}).to_string()}).to_string()});
    assert!(
        bridge
            .event(json!({"type":"response.completed","response":{"output":[native]}}))
            .is_err()
    );
    source.as_object_mut().unwrap().remove("tool_choice");
    for tools in [
        json!(null),
        json!({}),
        json!([{"type":"function","name":"shell","parameters":"invalid-schema"}]),
        json!([{"type":"function","name":"shell"},{"type":"function","name":"shell"}]),
    ] {
        source["input"][0]["tools"] = tools;
        assert!(BpsRequest::prepare(&source, "model", identity(), &[]).is_err());
    }
}

#[test]
fn tool_batch_is_validated_before_any_call_is_delivered() {
    let mut bridge = BpsResponse::new(request());
    let native = |name: &str| {
        json!({"type":"function_call","id":"fc_a","call_id":"call_a","name":"run_officejs",
        "arguments":json!({"code":json!({"name":name,"arguments":{"cmd":"echo hello"}}).to_string()}).to_string()})
    };
    assert!(
        bridge
            .event(
                json!({"type":"response.output_item.added","output_index":0,"item":native("shell")})
            )
            .unwrap()
            .is_empty()
    );
    assert!(bridge.event(json!({"type":"response.function_call_arguments.delta","output_index":0,"delta":"hidden envelope"})).unwrap().is_empty());
    assert!(bridge.event(json!({"type":"response.completed","response":{"id":"resp_a","output":[native("undeclared")]}})).is_err());
    assert!(bridge.replays().is_empty());
}

#[test]
fn tool_roundtrip_preserves_large_numbers_and_native_identity() {
    let mut bridge = BpsResponse::new(request());
    let args: Value = serde_json::from_str(r#"{"number":9007199254740993}"#).unwrap();
    let native = json!({"type":"function_call","id":"fc_native","call_id":"call_native","name":"run_officejs",
        "arguments":json!({"code":json!({"name":"shell","arguments":args}).to_string()}).to_string()});
    let events = bridge
        .event(json!({"type":"response.completed","response":{"id":"resp_a","output":[native]}}))
        .unwrap();
    let call = events.last().unwrap()["response"]["output"][0].clone();
    assert_eq!(call["name"], "shell");
    assert!(
        call["arguments"]
            .as_str()
            .unwrap()
            .contains("9007199254740993")
    );
    let continuation = BpsRequest::prepare(&json!({"input":[call,{"type":"function_call_output","call_id":"call_native","output":"done"}]}),
        "model", RequestIdentity {task_id:"task".into(),turn_id:"next".into(),prompt_cache_key:None},bridge.replays()).unwrap();
    let history = continuation.body()["input"].as_array().unwrap();
    assert_eq!(history[history.len() - 2], native);
    assert_eq!(history.last().unwrap()["call_id"], "call_native");
}

#[test]
fn output_only_history_requires_matching_native_replay() {
    let result = BpsRequest::prepare(
        &json!({"input":[{"type":"function_call_output","call_id":"missing","output":"private"}]}),
        "model",
        RequestIdentity {
            task_id: "t".into(),
            turn_id: "r".into(),
            prompt_cache_key: None,
        },
        &[],
    );
    assert!(result.is_err());
}

#[test]
fn custom_native_events_are_withheld_and_unknown_terminal_tools_rejected() {
    let mut bridge = BpsResponse::new(request());
    assert!(bridge.event(json!({"type":"response.output_item.added","output_index":0,"item":{"type":"custom_tool_call","name":"unexpected"}})).unwrap().is_empty());
    assert!(bridge.event(json!({"type":"response.custom_tool_call_input.delta","output_index":0,"delta":"private"})).unwrap().is_empty());
    assert!(bridge.event(json!({"type":"response.completed","response":{"output":[{"type":"custom_tool_call","name":"unexpected","input":"private"}]}})).is_err());
}

#[test]
fn tool_schema_rejects_invalid_arguments_and_external_resources() {
    let identity = || RequestIdentity {
        task_id: "t".into(),
        turn_id: "r".into(),
        prompt_cache_key: None,
    };
    assert!(BpsRequest::prepare(&json!({"input":"hello","tools":[{"type":"function","name":"shell","parameters":{"$ref":"https://example.invalid/schema"}}]}),"model",identity(),&[]).is_err());
    let request=BpsRequest::prepare(&json!({"input":"hello","tools":[{"type":"function","name":"shell","parameters":{"type":"object","required":["cmd"],"properties":{"cmd":{"type":"string"}}}}]}),"model",identity(),&[]).unwrap();
    let mut bridge = BpsResponse::new(request);
    let native = json!({"type":"function_call","call_id":"call_a","name":"run_officejs","arguments":json!({"code":json!({"name":"shell","arguments":{"cmd":42}}).to_string()}).to_string()});
    assert!(
        bridge
            .event(json!({"type":"response.completed","response":{"output":[native]}}))
            .is_err()
    );
    assert!(bridge.replays().is_empty());
}

#[test]
fn duplicate_calls_and_disabled_parallel_calls_reject_entire_batch() {
    for parallel in [true, false] {
        let request=BpsRequest::prepare(&json!({"input":"hello","parallel_tool_calls":parallel,"tools":[{"type":"function","name":"shell"}]}),"model",RequestIdentity{task_id:"t".into(),turn_id:"r".into(),prompt_cache_key:None},&[]).unwrap();
        let native = |id: &str| json!({"type":"function_call","call_id":id,"name":"run_officejs","arguments":json!({"code":json!({"name":"shell","arguments":{}}).to_string()}).to_string()});
        let mut bridge = BpsResponse::new(request);
        let second = if parallel { "call_a" } else { "call_b" };
        assert!(bridge.event(json!({"type":"response.completed","response":{"output":[native("call_a"),native(second)]}})).is_err());
        assert!(bridge.replays().is_empty());
    }
}

#[test]
fn namespaced_custom_call_preserves_raw_input() {
    let request=BpsRequest::prepare(&json!({"input":"hello","tools":[{"type":"namespace","name":"client","tools":[{"type":"custom","name":"patch"}]}]}),"model",RequestIdentity{task_id:"t".into(),turn_id:"r".into(),prompt_cache_key:None},&[]).unwrap();
    let input = "*** Begin Patch\n*** End Patch\n";
    let native = json!({"type":"function_call","call_id":"call_custom","name":"run_officejs","arguments":json!({"summary":"codex2api.custom/client.patch","code":input}).to_string()});
    let mut bridge = BpsResponse::new(request);
    let events = bridge
        .event(json!({"type":"response.completed","response":{"output":[native]}}))
        .unwrap();
    let call = &events.last().unwrap()["response"]["output"][0];
    assert_eq!(call["type"], "custom_tool_call");
    assert_eq!(call["name"], "patch");
    assert_eq!(call["namespace"], "client");
    assert_eq!(call["input"], input);
}

#[test]
fn unknown_native_output_items_are_rejected_before_delivery() {
    let mut bridge = BpsResponse::new(request());
    assert!(bridge.event(json!({"type":"response.output_item.added","output_index":0,"item":{"type":"web_search_call","id":"ws_private"}})).is_err());
}

#[test]
fn changed_history_arguments_do_not_reuse_cached_native_call() {
    let mut bridge = BpsResponse::new(request());
    let native = json!({"type":"function_call","id":"fc_old","call_id":"call_history","name":"run_officejs","arguments":json!({"code":json!({"name":"shell","arguments":{"cmd":"old"}}).to_string(),"summary":"native exact metadata"}).to_string()});
    let events = bridge
        .event(json!({"type":"response.completed","response":{"output":[native]}}))
        .unwrap();
    let mut call = events.last().unwrap()["response"]["output"][0].clone();
    call["arguments"] = json!({"cmd":"changed"}).to_string().into();
    let request = BpsRequest::prepare(
        &json!({"input":[call]}),
        "model",
        RequestIdentity {
            task_id: "t".into(),
            turn_id: "r".into(),
            prompt_cache_key: None,
        },
        bridge.replays(),
    )
    .unwrap();
    let replay = request.body()["input"].as_array().unwrap().last().unwrap();
    assert!(replay["arguments"].as_str().unwrap().contains("changed"));
    assert!(
        !replay["arguments"]
            .as_str()
            .unwrap()
            .contains("native exact metadata")
    );
}

#[test]
fn distinct_call_ids_cannot_share_a_native_output_item_id() {
    let mut bridge = BpsResponse::new(request());
    let native = |id: &str| json!({"type":"function_call","id":"fc_same","call_id":id,"name":"run_officejs","arguments":json!({"code":json!({"name":"shell","arguments":{}}).to_string()}).to_string()});
    assert!(bridge.event(json!({"type":"response.completed","response":{"output":[native("call_a"),native("call_b")]}})).is_err());
    assert!(bridge.replays().is_empty());
}

#[test]
fn historical_result_type_must_match_its_call() {
    let identity = || RequestIdentity {
        task_id: "t".into(),
        turn_id: "r".into(),
        prompt_cache_key: None,
    };
    assert!(BpsRequest::prepare(&json!({"input":[{"type":"custom_tool_call","call_id":"custom_a","name":"patch","input":"raw"},{"type":"function_call_output","call_id":"custom_a","output":"done"}]}),"model",identity(),&[]).is_err());
    assert!(BpsRequest::prepare(&json!({"input":[{"type":"function_call","call_id":"call_a","name":"shell","arguments":"{}"},{"type":"function_call_output","call_id":"call_a","output":"done"},{"type":"function_call_output","call_id":"call_a","output":"again"}]}),"model",identity(),&[]).is_err());
}

#[test]
fn function_arguments_preserve_code_branch_absence_and_empty_text() {
    for arguments in [
        json!({"action":"status"}),
        json!({"action":"run","code":""}),
    ] {
        let request=BpsRequest::prepare(&json!({"input":"hello","tools":[{"type":"function","name":"execute","parameters":{"oneOf":[{"type":"object","properties":{"action":{"const":"status"}},"required":["action"],"additionalProperties":false},{"type":"object","properties":{"action":{"const":"run"},"code":{"type":"string"}},"required":["action","code"],"additionalProperties":false}]}}]}),"model",RequestIdentity{task_id:"t".into(),turn_id:"r".into(),prompt_cache_key:None},&[]).unwrap();
        let native = json!({"type":"function_call","call_id":"call_branch","name":"run_officejs","arguments":json!({"code":json!({"name":"execute","arguments":arguments}).to_string()}).to_string()});
        let mut bridge = BpsResponse::new(request);
        let events = bridge
            .event(json!({"type":"response.completed","response":{"output":[native]}}))
            .unwrap();
        let actual: Value = serde_json::from_str(
            events.last().unwrap()["response"]["output"][0]["arguments"]
                .as_str()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(actual, arguments);
    }
}

#[test]
fn custom_history_lowers_item_ids_without_changing_call_pairing() {
    let request=BpsRequest::prepare(&json!({"input":[{"type":"custom_tool_call","id":"ctc_original","call_id":"client_pair","name":"patch","input":"raw patch"},{"type":"custom_tool_call_output","id":"ctco_original","call_id":"client_pair","output":"done"}]}),"model",RequestIdentity{task_id:"t".into(),turn_id:"r".into(),prompt_cache_key:None},&[]).unwrap();
    let history = request.body()["input"].as_array().unwrap();
    let (call, result) = (&history[history.len() - 2], history.last().unwrap());
    assert_eq!(call["call_id"], "client_pair");
    assert_eq!(result["call_id"], "client_pair");
    assert_eq!(result["type"], "function_call_output");
    for item in [call, result] {
        assert!(item["id"].as_str().unwrap().starts_with("fc_"));
        assert!(item["id"].as_str().unwrap().len() <= 64);
    }
    assert_ne!(call["id"], result["id"]);
}

#[test]
fn tool_instructions_bind_transport_to_declared_kind_including_namespaces() {
    let request=BpsRequest::prepare(&json!({"input":"hello","tools":[{"type":"namespace","name":"verification","tools":[{"type":"custom","name":"echo_raw"},{"type":"function","name":"echo_json","parameters":{"type":"object"}}]}]}),"model",RequestIdentity{task_id:"t".into(),turn_id:"r".into(),prompt_cache_key:None},&[]).unwrap();
    let instructions = request.body()["input"][0]["content"][0]["text"]
        .as_str()
        .unwrap();
    let catalog: Value =
        serde_json::from_str(instructions.split_once("Client tool catalog: ").unwrap().1).unwrap();
    let tools = catalog.as_array().unwrap();
    let custom = tools
        .iter()
        .find(|tool| tool["name"] == "verification.echo_raw")
        .unwrap();
    assert_eq!(custom["transport"]["kind"], "CUSTOM_RAW");
    assert_eq!(
        custom["transport"]["summary"],
        "codex2api.custom/verification.echo_raw"
    );
    let template = &custom["transport"]["outer_arguments_template"];
    assert_eq!(
        template["summary"],
        "codex2api.custom/verification.echo_raw"
    );
    assert_eq!(template["code"], "<EXACT_RAW_CUSTOM_INPUT>");
    assert!(template.get("arguments").is_none());
    assert_eq!(template["destructive"], false);
    assert_eq!(template["references"], json!([]));
    assert!(
        custom["transport"]["template_notice"]
            .as_str()
            .unwrap()
            .contains("never send the placeholder")
    );
    let function = tools
        .iter()
        .find(|tool| tool["name"] == "verification.echo_json")
        .unwrap();
    assert_eq!(function["transport"]["kind"], "FUNCTION_JSON");
    assert!(instructions.contains("Never wrap custom input in a function name/arguments envelope"));
    assert!(
        instructions.contains("a namespace changes only the qualified name, never the transport")
    );
}

#[test]
fn custom_json_looking_input_is_not_guessed_or_unwrapped() {
    let request=BpsRequest::prepare(&json!({"input":"hello","tools":[{"type":"namespace","name":"verification","tools":[{"type":"custom","name":"echo_raw"}]}]}),"model",RequestIdentity{task_id:"t".into(),turn_id:"r".into(),prompt_cache_key:None},&[]).unwrap();
    let raw = r#"{"name":"verification.echo_raw","arguments":{"input":"literal JSON"}}"#;
    let native = json!({"type":"function_call","call_id":"call_custom_json","name":"run_officejs","arguments":json!({"summary":"codex2api.custom/verification.echo_raw","code":raw}).to_string()});
    let mut bridge = BpsResponse::new(request);
    let events = bridge
        .event(json!({"type":"response.completed","response":{"output":[native]}}))
        .unwrap();
    assert_eq!(
        events.last().unwrap()["response"]["output"][0]["input"],
        raw
    );
}
