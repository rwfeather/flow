use crate::{
    integration_tests::harness::{draft_catalog, set_of, TestHarness, UserDiscoverResult},
    ControlPlane,
};
use proto_flow::capture::response::{discovered::Binding, Discovered};
use serde_json::json;

#[tokio::test]
#[serial_test::serial]
async fn test_auto_discovers_update_only() {
    let mut harness = TestHarness::init("test_auto_discovers").await;

    let user_id = harness.setup_tenant("pikas").await;

    let init_draft = draft_catalog(json!({
        "captures": {
            "pikas/capture": {
                "autoDiscover": {
                    "addNewCollections": false,
                    "evolveIncompatibleCollections": false,
                },
                "shards": {
                    "logLevel": "debug"
                },
                "interval": "42s",
                "endpoint": {
                    "connector": {
                        "image": "source/test:test",
                        "config": { "squeak": "squeak" }
                    },
                },
                "bindings": [
                    {
                        "resource": { "id": "grass", "extra": "grass" },
                        "target": "pikas/alpine-grass"
                    },
                    {
                        "resource": { "id": "moss", "extra": "moss" },
                        "target": "pikas/moss"
                    },
                    {
                        "resource": { "id": "lichen", "extra": "lichen" },
                        "target": "pikas/lichen"
                    }
                ]
            }
        },
        "collections": {
            "pikas/alpine-grass": {
                "schema": document_schema(1),
                "key": ["/id"]
            },
            "pikas/moss": {
                "schema": document_schema(1),
                "key": ["/id"]
            },
            "pikas/lichen": {
                "writeSchema": document_schema(1),
                "readSchema": models::Schema::default_inferred_read_schema(),
                "key": ["/id"]
            }
        }
    }));

    let result = harness
        .user_publication(user_id, "init publication", init_draft)
        .await;
    assert!(result.status.is_success());
}

fn document_schema(version: usize) -> String {
    serde_json::to_string(&serde_json::json!({
        "type": "object",
        "properties": {
            "id": {"type": "string"},
            "squeaks": { "type": "number", "maximum": version },
        },
        "required": ["id"]
    }))
    .unwrap()
}
