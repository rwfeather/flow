use crate::integration_tests::harness::{draft_catalog, TestHarness};
use proto_flow::capture::response::{discovered::Binding, Discovered};

#[tokio::test]
#[serial_test::serial]
async fn test_user_discovers() {
    let mut harness = TestHarness::init("test_user_discovers").await;

    let user_id = harness.setup_tenant("squirrels").await;

    let initial_resp = Discovered {
        bindings: vec![
            Binding {
                recommended_name: String::from("acorns"),
                document_schema_json: document_schema(1),
                resource_config_json: String::from(r#"{"id": "acorns"}"#),
                key: vec!["/id".to_string()],
                disable: false,
                resource_path: Vec::new(), // deprecated field
            },
            Binding {
                recommended_name: String::from("walnuts"),
                document_schema_json: document_schema(1),
                resource_config_json: String::from(r#"{"id": "walnuts"}"#),
                key: vec!["/id".to_string()],
                disable: false,
                resource_path: Vec::new(), // deprecated field
            },
            Binding {
                recommended_name: String::from("crab apples"),
                document_schema_json: document_schema(1),
                resource_config_json: String::from(r#"{"id": "crab apples"}"#),
                key: vec!["/id".to_string()],
                disable: true,
                resource_path: Vec::new(), // deprecated field
            },
        ],
    };
    // Start with an empty draft
    let draft_id = harness
        .create_draft(user_id, "initial", Default::default())
        .await;

    let endpoint_config = r#"{"tail": "shake"}"#;
    let result = harness
        .user_discover(
            "source/test",
            ":test",
            "squirrels/capture-1",
            draft_id,
            endpoint_config,
            false,
            Box::new(Ok::<Discovered, String>(initial_resp)),
        )
        .await;
    assert!(
        result.job_status.is_success(),
        "expected success, got: {:?}",
        result.job_status
    );
    assert!(result.errors.is_empty());
    assert_eq!(
        3,
        result.draft.collections.len(),
        "expected 3 collections in draft: {:?}",
        result.draft
    );

    insta::assert_debug_snapshot!("initial-discover", result.draft);

    let pub_result = harness
        .create_user_publication(user_id, draft_id, "initial publication")
        .await;

    assert!(pub_result.status.is_success());
    let published_specs = pub_result
        .live_specs
        .into_iter()
        .map(|ls| (ls.catalog_name, ls.spec_type, ls.spec))
        .collect::<Vec<_>>();
    // Expect to see only the two enabled collections. The `crab apples` should have been pruned.
    insta::assert_debug_snapshot!("initial-publication", published_specs);

    // Now discover again, and have it return some different collections so we
    // can test the merge behavior. Start with some changes already in the
    // draft, so we can assert that the merge handles those properly.
    let draft_id = harness
        .create_draft(
            user_id,
            "second discover",
            draft_catalog(serde_json::json!({
                "captures": {
                    "squirrels/capture-1": {
                        "endpoint": {
                            "image": "drafted/different/image:tag",
                            "config": { "drafted": {"config": "different" }}
                        },
                        "bindings": [
                            // Acorns binding was removed, and should be added back
                            {
                                "resource": { "id": "walnuts"},
                                "target": "squirrels/drafted-collection",
                                "disable": true // should remain disabled after merge
                            },
                            {
                                "resource": { "id": "drafted"},
                                "target": "squirrels/drafted-collection"
                            }
                        ]
                    }
                },
                "collections": {
                    "squirrels/acorns": {
                        "schema": {
                            "type": "object",
                            "properties": {
                                "id": { "type": "string" },
                                "drafted": { "type": "string" }
                            },
                            "required": ["id", "drafted"]
                        },
                        "projections": {
                            "iiiiiideeeee": "/id"
                        },
                        "key": ["/drafted"]
                    },
                    "squirrels/walnuts": {
                        "writeSchema": {
                            "type": "object",
                            "properties": {
                                "id": { "type": "string" },
                                "drafted": { "type": "string" }
                            },
                            "required": ["id", "drafted"]
                        },
                        "readSchema": {
                            "type": "object",
                            "properties": {
                                "id": { "type": "string" },
                                "drafted": { "type": "string" }
                            },
                            "required": ["id", "drafted"]
                        },
                        "key": ["/drafted"]
                    },
                    "squirrels/extra": {
                        "schema": {
                            "type": "object",
                            "properties": {
                                "id": { "type": "string" },
                            },
                            "required": ["id"]
                        },
                        "key": ["/id"]
                    }
                }
            })),
        )
        .await;

    let next_discover = Discovered {
        bindings: vec![
            Binding {
                recommended_name: String::from("acorns"),
                document_schema_json: document_schema(2),
                resource_config_json: String::from(r#"{"id": "acorns"}"#),
                key: vec!["/id".to_string()],
                disable: false,
                resource_path: Vec::new(), // deprecated field
            },
            Binding {
                recommended_name: String::from("walnuts"),
                document_schema_json: document_schema(2),
                resource_config_json: String::from(r#"{"id": "walnuts"}"#),
                key: vec!["/id".to_string()],
                disable: false,
                resource_path: Vec::new(), // deprecated field
            },
            Binding {
                recommended_name: String::from("hickory nuts!"),
                document_schema_json: document_schema(2),
                resource_config_json: String::from(r#"{"id": "hickory-nuts"}"#),
                key: vec!["/id".to_string()],
                disable: false,
                resource_path: Vec::new(), // deprecated field
            },
        ],
    };
    let endpoint_config = r##"{ "newConfig": "forDiscover" }"##;
    let result = harness
        .user_discover(
            "source/test",
            ":test",
            "squirrels/capture-1",
            draft_id,
            endpoint_config,
            true,
            Box::new(Ok(next_discover)),
        )
        .await;
    assert!(result.job_status.is_success());
    insta::assert_debug_snapshot!("second-discover", result.draft);
}

fn document_schema(version: usize) -> String {
    serde_json::to_string(&serde_json::json!({
        "type": "object",
        "properties": {
            "id": {"type": "string"},
            "nuttiness": { "type": "number", "maximum": version },
        },
        "required": ["id"]
    }))
    .unwrap()
}
