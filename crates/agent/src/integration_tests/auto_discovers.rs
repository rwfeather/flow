use std::time::Duration;

use crate::{
    discovers::{Changes, ResourcePath},
    integration_tests::harness::{
        draft_catalog, set_of, InjectBuildError, TestHarness, UserDiscoverResult,
    },
    publications, ControlPlane,
};
use proto_flow::capture::response::{discovered::Binding, Discovered};
use serde_json::json;

// Testing auto-discovers is a bit tricky because we don't really have the
// ability to "fast-forward" time. We have to just set the interval to something
// very low and wait until it's due. We can't use a 0 interval, though, because
// then the auto-discover will run literally every time the controller runs,
// even when it's just supposed to activate. So we resign ourselves to `sleep`,
// and just try to make it as fast as possible.
const AUTO_DISCOVER_WAIT: Duration = Duration::from_millis(20);
const AUTO_DISCOVER_INTERVAL: &str = "15ms";

#[tokio::test]
#[serial_test::serial]
async fn test_auto_discovers_update_only() {
    let mut harness = TestHarness::init("test_auto_discovers").await;

    let user_id = harness.setup_tenant("pikas").await;

    let init_draft = draft_catalog(json!({
        "captures": {
            "pikas/capture": {
                "autoDiscover": {
                    "addNewBindings": false,
                    "evolveIncompatibleCollections": true,
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
                        "target": "pikas/lichen",
                        "disable": true,
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
        },
        "materializations": {
            "pikas/materialize": {
                "sourceCapture": "pikas/capture",
                "endpoint": {
                    "connector": {
                        "image": "materialize/test:test",
                        "config": { "squeak": "squeak squeak" }
                    }
                },
                "bindings": [] // let the materialization controller fill them in
            }
        }
    }));

    let result = harness
        .user_publication(user_id, "init publication", init_draft)
        .await;
    assert!(result.status.is_success());

    harness.run_pending_controllers(None).await;

    // Expect to see that the controller has initialized a blank auto-capture status.
    let capture_state = harness.get_controller_state("pikas/capture").await;
    assert!(capture_state.next_run.is_some());
    assert!(capture_state
        .current_status
        .unwrap_capture()
        .auto_discover
        .is_some());

    harness
        .set_auto_discover_interval("pikas/capture", AUTO_DISCOVER_INTERVAL)
        .await;
    tokio::time::sleep(AUTO_DISCOVER_WAIT).await;
    let discovered = Discovered {
        bindings: vec![
            Binding {
                recommended_name: "grass".to_string(),
                resource_config_json: r#"{"id": "grass"}"#.to_string(),
                document_schema_json: document_schema(2).to_string(),
                key: vec!["/id".to_string()],
                disable: true,
                resource_path: Vec::new(),
            },
            Binding {
                recommended_name: "moss".to_string(),
                resource_config_json:
                    r#"{"id": "moss", "expect": "existing config takes precedence" }"#.to_string(),
                document_schema_json: document_schema(1).to_string(),
                key: vec!["/id".to_string()],
                disable: true,
                resource_path: Vec::new(),
            },
            Binding {
                recommended_name: "lichen".to_string(),
                resource_config_json: r#"{"id": "lichen"}"#.to_string(),
                document_schema_json: document_schema(1).to_string(),
                key: vec!["/id".to_string()],
                disable: false,
                resource_path: Vec::new(),
            },
        ],
    };
    harness
        .discover_handler
        .connectors
        .mock_discover(Box::new(Ok(discovered)));

    tracing::info!("TEST about to run discover");
    harness.run_pending_controller("pikas/capture").await;
    let capture_state = harness.get_controller_state("pikas/capture").await;
    let auto_discover = capture_state
        .current_status
        .unwrap_capture()
        .auto_discover
        .as_ref()
        .unwrap();

    assert!(auto_discover.failure.is_none());
    assert!(auto_discover.last_success.is_some());
    let last_success = auto_discover.last_success.as_ref().unwrap();

    assert_eq!(
        &changes(&[(&["grass"], "pikas/alpine-grass"),]),
        &last_success.modified
    );
    assert!(last_success.added.is_empty());
    assert!(last_success.removed.is_empty());
    assert!(last_success
        .publish_result
        .as_ref()
        .is_some_and(|pr| pr.is_success()));
    let last_disco_time = last_success.ts;

    // Discover again with the same response, and assert that there are no changes, and no publication.
    tokio::time::sleep(AUTO_DISCOVER_WAIT).await;
    harness.run_pending_controller("pikas/capture").await;

    let capture_state = harness.get_controller_state("pikas/capture").await;
    let auto_discover = capture_state
        .current_status
        .unwrap_capture()
        .auto_discover
        .as_ref()
        .unwrap();

    assert!(auto_discover.failure.is_none());
    assert!(auto_discover.last_success.is_some());
    let last_success = auto_discover.last_success.as_ref().unwrap();
    assert!(last_success.ts > last_disco_time);
    assert!(last_success.added.is_empty());
    assert!(last_success.modified.is_empty());
    assert!(last_success.removed.is_empty());
    assert!(last_success.publish_result.is_none());

    // Now simulate a discover error, and expect to see the error status reported.
    harness
        .discover_handler
        .connectors
        .mock_discover(Box::new(Err("a simulated discover error".to_string())));
    tokio::time::sleep(AUTO_DISCOVER_WAIT).await;
    harness.run_pending_controller("pikas/capture").await;

    let capture_state = harness.get_controller_state("pikas/capture").await;
    let auto_discover = capture_state
        .current_status
        .unwrap_capture()
        .auto_discover
        .as_ref()
        .unwrap();
    assert!(auto_discover.failure.is_some());
    let failure = auto_discover.failure.as_ref().unwrap();
    assert!(failure.last_outcome.errors[0]
        .detail
        .contains("a simulated discover error"));
    assert_eq!(1, failure.count);

    // Now simulate a subsequent successful discover, but with a failure to
    // publish. We'll expect to see the error count go up.
    let discovered = Discovered {
        bindings: vec![
            Binding {
                recommended_name: "grass".to_string(),
                resource_config_json: r#"{"id": "grass"}"#.to_string(),
                document_schema_json: document_schema(2).to_string(),
                key: vec!["/id".to_string()],
                disable: false,
                resource_path: Vec::new(),
            },
            Binding {
                recommended_name: "moss".to_string(),
                resource_config_json:
                    r#"{"id": "moss", "expect": "existing config takes precedence" }"#.to_string(),
                document_schema_json: document_schema(2).to_string(),
                key: vec!["/id".to_string()],
                disable: true,
                resource_path: Vec::new(),
            },
            // Lichens is missing, and we expect the corresponding binding to be
            // removed once a successful discover is published.
        ],
    };
    harness
        .discover_handler
        .connectors
        .mock_discover(Box::new(Ok(discovered)));
    harness.control_plane().fail_next_build(
        "pikas/capture",
        InjectBuildError::new(
            tables::synthetic_scope(models::CatalogType::Capture, "pikas/capture"),
            anyhow::anyhow!("a simulated build failure"),
        ),
    );
    tokio::time::sleep(AUTO_DISCOVER_WAIT).await;
    harness.run_pending_controller("pikas/capture").await;

    let capture_state = harness.get_controller_state("pikas/capture").await;
    let auto_discover = capture_state
        .current_status
        .unwrap_capture()
        .auto_discover
        .as_ref()
        .unwrap();
    assert!(auto_discover.failure.is_some());
    let failure = auto_discover.failure.as_ref().unwrap();
    assert_eq!(2, failure.count);
    assert_eq!(
        Some(publications::JobStatus::BuildFailed {
            incompatible_collections: Vec::new(),
            evolution_id: None
        }),
        failure.last_outcome.publish_result
    );
    // Ensure that the failed publication is shown in the history.
    let pub_history = capture_state
        .current_status
        .unwrap_capture()
        .publications
        .history
        .front()
        .unwrap();
    assert!(pub_history.errors[0]
        .detail
        .contains("a simulated build failure"));
    let last_fail_time = failure.last_outcome.ts;

    // Now this time, we'll discover a changed key, and expect that the initial publication fails
    // due to the key change, and that a subsequent publication of a _v2 collection is successful.
    let discovered = Discovered {
        bindings: vec![
            Binding {
                recommended_name: "grass".to_string(),
                resource_config_json: r#"{"id": "grass"}"#.to_string(),
                document_schema_json: document_schema(2).to_string(),
                key: vec!["/id".to_string(), "/squeaks".to_string()],
                disable: false,
                resource_path: Vec::new(),
            },
            Binding {
                recommended_name: "moss".to_string(),
                resource_config_json:
                    r#"{"id": "moss", "expect": "existing config takes precedence" }"#.to_string(),
                document_schema_json: document_schema(2).to_string(),
                key: vec!["/id".to_string()],
                disable: true,
                resource_path: Vec::new(),
            },
            // Lichens is missing, and we expect the corresponding binding to be
            // removed once a successful discover is published.
        ],
    };
    harness
        .discover_handler
        .connectors
        .mock_discover(Box::new(Ok(discovered)));
    tokio::time::sleep(AUTO_DISCOVER_WAIT).await;
    harness.run_pending_controller("pikas/capture").await;
    //harness.run_pending_controllers(None)

    let capture_state = harness.get_controller_state("pikas/capture").await;
    let auto_discover = capture_state
        .current_status
        .unwrap_capture()
        .auto_discover
        .as_ref()
        .unwrap();
    let last_success = auto_discover.last_success.as_ref().unwrap();
    assert!(last_success.ts > last_fail_time);
}

fn changes(c: &[(&[&str], &str)]) -> Vec<(ResourcePath, models::Collection)> {
    c.into_iter()
        .map(|(path, value)| {
            (
                path.iter().map(|s| s.to_string()).collect(),
                models::Collection::new(*value),
            )
        })
        .collect()
}

fn document_schema(version: usize) -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "id": {"type": "string"},
            "squeaks": { "type": "number", "maximum": version },
        },
        "required": ["id", "squeaks"]
    })
}
