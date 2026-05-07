use codex_api::OpenAiVerbosity;
use codex_api::ResponsesApiRequest;
use codex_api::TextControls;
use codex_api::Thinking;
use codex_api::ThinkingType;
use codex_api::create_text_param_for_request;
use codex_protocol::config_types::ServiceTier;
use codex_protocol::models::FunctionCallOutputPayload;
use codex_protocol::models::ReasoningItemContent;
use pretty_assertions::assert_eq;

use super::*;

#[test]
fn serializes_text_verbosity_when_set() {
    let input: Vec<ResponseItem> = vec![];
    let tools: Vec<serde_json::Value> = vec![];
    let req = ResponsesApiRequest {
        model: "gpt-5.4".to_string(),
        instructions: "i".to_string(),
        input,
        tools,
        tool_choice: "auto".to_string(),
        parallel_tool_calls: true,
        reasoning: None,
        store: false,
        stream: true,
        include: vec![],
        prompt_cache_key: None,
        service_tier: None,
        thinking: None,
        cache: None,
        cache_key: None,
        text: Some(TextControls {
            verbosity: Some(OpenAiVerbosity::Low),
            format: None,
        }),
        client_metadata: None,
    };

    let v = serde_json::to_value(&req).expect("json");
    assert_eq!(
        v.get("text")
            .and_then(|t| t.get("verbosity"))
            .and_then(|s| s.as_str()),
        Some("low")
    );
}

#[test]
fn serializes_text_schema_with_strict_format() {
    let input: Vec<ResponseItem> = vec![];
    let tools: Vec<serde_json::Value> = vec![];
    let schema = serde_json::json!({
        "type": "object",
        "properties": {
            "answer": {"type": "string"}
        },
        "required": ["answer"],
    });
    let text_controls = create_text_param_for_request(
        /*verbosity*/ None,
        &Some(schema.clone()),
        /*output_schema_strict*/ true,
    )
    .expect("text controls");

    let req = ResponsesApiRequest {
        model: "gpt-5.4".to_string(),
        instructions: "i".to_string(),
        input,
        tools,
        tool_choice: "auto".to_string(),
        parallel_tool_calls: true,
        reasoning: None,
        store: false,
        stream: true,
        include: vec![],
        prompt_cache_key: None,
        service_tier: None,
        thinking: None,
        cache: None,
        cache_key: None,
        text: Some(text_controls),
        client_metadata: None,
    };

    let v = serde_json::to_value(&req).expect("json");
    let text = v.get("text").expect("text field");
    assert!(text.get("verbosity").is_none());
    let format = text.get("format").expect("format field");

    assert_eq!(
        format.get("name"),
        Some(&serde_json::Value::String("codex_output_schema".into()))
    );
    assert_eq!(
        format.get("type"),
        Some(&serde_json::Value::String("json_schema".into()))
    );
    assert_eq!(format.get("strict"), Some(&serde_json::Value::Bool(true)));
    assert_eq!(format.get("schema"), Some(&schema));
}

#[test]
fn serializes_text_schema_with_non_strict_format() {
    let schema = serde_json::json!({
        "type": "object",
        "properties": {
            "answer": {"type": "string"},
            "rationale": {"type": "string"}
        },
        "required": ["answer"],
        "additionalProperties": false
    });
    let text_controls = create_text_param_for_request(
        /*verbosity*/ None,
        &Some(schema.clone()),
        /*output_schema_strict*/ false,
    )
    .expect("text controls");

    let format = text_controls.format.expect("format field");
    assert!(!format.strict);
    assert_eq!(format.schema, schema);
}

#[test]
fn omits_text_when_not_set() {
    let input: Vec<ResponseItem> = vec![];
    let tools: Vec<serde_json::Value> = vec![];
    let req = ResponsesApiRequest {
        model: "gpt-5.4".to_string(),
        instructions: "i".to_string(),
        input,
        tools,
        tool_choice: "auto".to_string(),
        parallel_tool_calls: true,
        reasoning: None,
        store: false,
        stream: true,
        include: vec![],
        prompt_cache_key: None,
        service_tier: None,
        thinking: None,
        cache: None,
        cache_key: None,
        text: None,
        client_metadata: None,
    };

    let v = serde_json::to_value(&req).expect("json");
    assert!(v.get("text").is_none());
}

#[test]
fn serializes_service_tier_when_set() {
    let req = ResponsesApiRequest {
        model: "gpt-5.4".to_string(),
        instructions: "i".to_string(),
        input: vec![],
        tools: vec![],
        tool_choice: "auto".to_string(),
        parallel_tool_calls: true,
        reasoning: None,
        store: false,
        stream: true,
        include: vec![],
        prompt_cache_key: None,
        service_tier: Some(ServiceTier::Priority.to_string()),
        thinking: None,
        cache: None,
        cache_key: None,
        text: None,
        client_metadata: None,
    };

    let v = serde_json::to_value(&req).expect("json");
    assert_eq!(
        v.get("service_tier").and_then(|tier| tier.as_str()),
        Some("priority")
    );
}

#[test]
fn serializes_batch_service_tier_when_set() {
    let req = ResponsesApiRequest {
        model: "gpt-5.1".to_string(),
        instructions: "i".to_string(),
        input: vec![],
        tools: vec![],
        tool_choice: "auto".to_string(),
        parallel_tool_calls: true,
        reasoning: None,
        store: false,
        stream: true,
        include: vec![],
        prompt_cache_key: None,
        service_tier: Some(ServiceTier::Batch.to_string()),
        thinking: None,
        cache: None,
        cache_key: None,
        text: None,
        client_metadata: None,
    };

    let v = serde_json::to_value(&req).expect("json");
    assert_eq!(
        v.get("service_tier").and_then(|tier| tier.as_str()),
        Some("batch")
    );
}

#[test]
fn responses_request_preserves_raw_reasoning_across_tool_call_rounds() {
    let req = ResponsesApiRequest {
        model: "MiniMax-M2.7".to_string(),
        instructions: String::new(),
        input: vec![
            ResponseItem::Reasoning {
                id: "rs_minimax".to_string(),
                summary: Vec::new(),
                content: Some(vec![ReasoningItemContent::ReasoningText {
                    text: "I should call the weather tool before answering.".to_string(),
                }]),
                encrypted_content: None,
            },
            ResponseItem::FunctionCall {
                id: Some("fc_minimax".to_string()),
                name: "get_weather".to_string(),
                namespace: None,
                arguments: r#"{"location":"Paris"}"#.to_string(),
                call_id: "call_weather".to_string(),
            },
            ResponseItem::FunctionCallOutput {
                call_id: "call_weather".to_string(),
                output: FunctionCallOutputPayload::from_text("22 C and sunny".to_string()),
            },
        ],
        tools: vec![],
        tool_choice: "auto".to_string(),
        parallel_tool_calls: false,
        reasoning: None,
        store: false,
        stream: true,
        include: vec![],
        prompt_cache_key: None,
        service_tier: None,
        thinking: None,
        cache: None,
        cache_key: None,
        text: None,
        client_metadata: None,
    };

    let serialized = serde_json::to_value(&req).expect("request should serialize");

    assert_eq!(
        serialized["input"],
        serde_json::json!([
            {
                "type": "reasoning",
                "summary": [],
                "content": [{
                    "type": "reasoning_text",
                    "text": "I should call the weather tool before answering.",
                }],
                "encrypted_content": null,
            },
            {
                "type": "function_call",
                "name": "get_weather",
                "arguments": "{\"location\":\"Paris\"}",
                "call_id": "call_weather",
            },
            {
                "type": "function_call_output",
                "call_id": "call_weather",
                "output": "22 C and sunny",
            },
        ])
    );
}

#[test]
fn responses_request_preserves_glm_and_kimi_raw_reasoning_across_tool_call_rounds() {
    for (model, reasoning_text) in [
        (
            "glm-5.1",
            "GLM should keep this reasoning for the next tool round.",
        ),
        (
            "kimi-k2.5",
            "Kimi should keep this reasoning for the next tool round.",
        ),
    ] {
        let req = ResponsesApiRequest {
            model: model.to_string(),
            instructions: String::new(),
            input: vec![
                ResponseItem::Reasoning {
                    id: "rs_raw_reasoning".to_string(),
                    summary: Vec::new(),
                    content: Some(vec![ReasoningItemContent::ReasoningText {
                        text: reasoning_text.to_string(),
                    }]),
                    encrypted_content: None,
                },
                ResponseItem::FunctionCall {
                    id: Some("fc_tool".to_string()),
                    name: "get_weather".to_string(),
                    namespace: None,
                    arguments: r#"{"location":"Paris"}"#.to_string(),
                    call_id: "call_weather".to_string(),
                },
                ResponseItem::FunctionCallOutput {
                    call_id: "call_weather".to_string(),
                    output: FunctionCallOutputPayload::from_text("22 C and sunny".to_string()),
                },
            ],
            tools: vec![],
            tool_choice: "auto".to_string(),
            parallel_tool_calls: false,
            reasoning: None,
            store: false,
            stream: true,
            include: vec![],
            prompt_cache_key: None,
            service_tier: None,
            thinking: None,
            cache: None,
            cache_key: None,
            text: None,
            client_metadata: None,
        };

        let serialized = serde_json::to_value(&req).expect("request should serialize");

        assert_eq!(serialized["model"], serde_json::json!(model));
        assert_eq!(
            serialized["input"],
            serde_json::json!([
                {
                    "type": "reasoning",
                    "summary": [],
                    "content": [{
                        "type": "reasoning_text",
                        "text": reasoning_text,
                    }],
                    "encrypted_content": null,
                },
                {
                    "type": "function_call",
                    "name": "get_weather",
                    "arguments": "{\"location\":\"Paris\"}",
                    "call_id": "call_weather",
                },
                {
                    "type": "function_call_output",
                    "call_id": "call_weather",
                    "output": "22 C and sunny",
                },
            ])
        );
    }
}

#[test]
fn responses_request_preserves_glm_thinking_cache_and_raw_reasoning() {
    let req = ResponsesApiRequest {
        model: "glm-5.1".to_string(),
        instructions: String::new(),
        input: vec![
            ResponseItem::Reasoning {
                id: "rs_glm".to_string(),
                summary: Vec::new(),
                content: Some(vec![ReasoningItemContent::ReasoningText {
                    text: "GLM preserved thinking from the previous model response.".to_string(),
                }]),
                encrypted_content: None,
            },
            ResponseItem::FunctionCall {
                id: Some("fc_glm".to_string()),
                name: "get_weather".to_string(),
                namespace: None,
                arguments: r#"{"location":"Paris"}"#.to_string(),
                call_id: "call_weather".to_string(),
            },
            ResponseItem::FunctionCallOutput {
                call_id: "call_weather".to_string(),
                output: FunctionCallOutputPayload::from_text("22 C and sunny".to_string()),
            },
        ],
        tools: vec![],
        tool_choice: "auto".to_string(),
        parallel_tool_calls: false,
        reasoning: None,
        store: false,
        stream: true,
        include: vec![],
        prompt_cache_key: None,
        service_tier: None,
        thinking: Some(Thinking {
            r#type: ThinkingType::Enabled,
            clear_thinking: false,
            mode: Some("deep".to_string()),
            level: Some(2),
        }),
        cache: Some(true),
        cache_key: Some("glm-shared-cache".to_string()),
        text: None,
        client_metadata: None,
    };

    let serialized = serde_json::to_value(&req).expect("request should serialize");

    assert_eq!(
        serialized,
        serde_json::json!({
            "model": "glm-5.1",
            "input": [
                {
                    "type": "reasoning",
                    "summary": [],
                    "content": [{
                        "type": "reasoning_text",
                        "text": "GLM preserved thinking from the previous model response.",
                    }],
                    "encrypted_content": null,
                },
                {
                    "type": "function_call",
                    "name": "get_weather",
                    "arguments": "{\"location\":\"Paris\"}",
                    "call_id": "call_weather",
                },
                {
                    "type": "function_call_output",
                    "call_id": "call_weather",
                    "output": "22 C and sunny",
                },
            ],
            "tools": [],
            "tool_choice": "auto",
            "parallel_tool_calls": false,
            "reasoning": null,
            "store": false,
            "stream": true,
            "include": [],
            "thinking": {
                "type": "enabled",
                "clear_thinking": false,
                "mode": "deep",
                "level": 2,
            },
            "cache": true,
            "cache_key": "glm-shared-cache",
        })
    );
}

#[test]
fn responses_request_preserves_kimi_raw_reasoning_without_glm_only_controls() {
    let req = ResponsesApiRequest {
        model: "kimi-k2.5".to_string(),
        instructions: String::new(),
        input: vec![
            ResponseItem::Reasoning {
                id: "rs_kimi".to_string(),
                summary: Vec::new(),
                content: Some(vec![ReasoningItemContent::ReasoningText {
                    text: "Kimi preserved thinking from the previous model response.".to_string(),
                }]),
                encrypted_content: None,
            },
            ResponseItem::FunctionCall {
                id: Some("fc_kimi".to_string()),
                name: "get_weather".to_string(),
                namespace: None,
                arguments: r#"{"location":"Paris"}"#.to_string(),
                call_id: "call_weather".to_string(),
            },
            ResponseItem::FunctionCallOutput {
                call_id: "call_weather".to_string(),
                output: FunctionCallOutputPayload::from_text("22 C and sunny".to_string()),
            },
        ],
        tools: vec![],
        tool_choice: "auto".to_string(),
        parallel_tool_calls: false,
        reasoning: None,
        store: false,
        stream: true,
        include: vec![],
        prompt_cache_key: None,
        service_tier: None,
        thinking: None,
        cache: None,
        cache_key: None,
        text: None,
        client_metadata: None,
    };

    let serialized = serde_json::to_value(&req).expect("request should serialize");

    assert_eq!(
        serialized,
        serde_json::json!({
            "model": "kimi-k2.5",
            "input": [
                {
                    "type": "reasoning",
                    "summary": [],
                    "content": [{
                        "type": "reasoning_text",
                        "text": "Kimi preserved thinking from the previous model response.",
                    }],
                    "encrypted_content": null,
                },
                {
                    "type": "function_call",
                    "name": "get_weather",
                    "arguments": "{\"location\":\"Paris\"}",
                    "call_id": "call_weather",
                },
                {
                    "type": "function_call_output",
                    "call_id": "call_weather",
                    "output": "22 C and sunny",
                },
            ],
            "tools": [],
            "tool_choice": "auto",
            "parallel_tool_calls": false,
            "reasoning": null,
            "store": false,
            "stream": true,
            "include": [],
        })
    );
}

#[test]
fn responses_request_preserves_encrypted_reasoning_across_tool_call_rounds() {
    let req = ResponsesApiRequest {
        model: "minimax-m2.7".to_string(),
        instructions: String::new(),
        input: vec![
            ResponseItem::Reasoning {
                id: "rs_ollama_minimax".to_string(),
                summary: Vec::new(),
                content: None,
                encrypted_content: Some(
                    "I should call the weather tool before answering.".to_string(),
                ),
            },
            ResponseItem::FunctionCall {
                id: Some("fc_ollama_minimax".to_string()),
                name: "get_weather".to_string(),
                namespace: None,
                arguments: r#"{"location":"Paris"}"#.to_string(),
                call_id: "call_weather".to_string(),
            },
            ResponseItem::FunctionCallOutput {
                call_id: "call_weather".to_string(),
                output: FunctionCallOutputPayload::from_text("22 C and sunny".to_string()),
            },
        ],
        tools: vec![],
        tool_choice: "auto".to_string(),
        parallel_tool_calls: false,
        reasoning: None,
        store: false,
        stream: true,
        include: vec![],
        prompt_cache_key: None,
        service_tier: None,
        thinking: None,
        cache: None,
        cache_key: None,
        text: None,
        client_metadata: None,
    };

    let serialized = serde_json::to_value(&req).expect("request should serialize");

    assert_eq!(
        serialized["input"],
        serde_json::json!([
            {
                "type": "reasoning",
                "summary": [],
                "encrypted_content": "I should call the weather tool before answering.",
            },
            {
                "type": "function_call",
                "name": "get_weather",
                "arguments": "{\"location\":\"Paris\"}",
                "call_id": "call_weather",
            },
            {
                "type": "function_call_output",
                "call_id": "call_weather",
                "output": "22 C and sunny",
            },
        ])
    );
}

#[test]
fn reserializes_shell_outputs_for_function_and_custom_tool_calls() {
    let raw_output = r#"{"output":"hello","metadata":{"exit_code":0,"duration_seconds":0.5}}"#;
    let expected_output = "Exit code: 0\nWall time: 0.5 seconds\nOutput:\nhello";
    let mut items = vec![
        ResponseItem::FunctionCall {
            id: None,
            name: "shell".to_string(),
            namespace: None,
            arguments: "{}".to_string(),
            call_id: "call-1".to_string(),
        },
        ResponseItem::FunctionCallOutput {
            call_id: "call-1".to_string(),
            output: FunctionCallOutputPayload::from_text(raw_output.to_string()),
        },
        ResponseItem::CustomToolCall {
            id: None,
            status: None,
            call_id: "call-2".to_string(),
            name: "apply_patch".to_string(),
            input: "*** Begin Patch".to_string(),
        },
        ResponseItem::CustomToolCallOutput {
            call_id: "call-2".to_string(),
            name: None,
            output: FunctionCallOutputPayload::from_text(raw_output.to_string()),
        },
    ];

    reserialize_shell_outputs(&mut items);

    assert_eq!(
        items,
        vec![
            ResponseItem::FunctionCall {
                id: None,
                name: "shell".to_string(),
                namespace: None,
                arguments: "{}".to_string(),
                call_id: "call-1".to_string(),
            },
            ResponseItem::FunctionCallOutput {
                call_id: "call-1".to_string(),
                output: FunctionCallOutputPayload::from_text(expected_output.to_string()),
            },
            ResponseItem::CustomToolCall {
                id: None,
                status: None,
                call_id: "call-2".to_string(),
                name: "apply_patch".to_string(),
                input: "*** Begin Patch".to_string(),
            },
            ResponseItem::CustomToolCallOutput {
                call_id: "call-2".to_string(),
                name: None,
                output: FunctionCallOutputPayload::from_text(expected_output.to_string()),
            },
        ]
    );
}
