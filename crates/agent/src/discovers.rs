use std::collections::{BTreeMap, HashSet};

use super::logs;
use anyhow::Context;
use models::{split_image_tag, CaptureEndpoint};
use proto_flow::{capture, flow::capture_spec};
use sqlx::{types::Uuid, PgPool};

pub(crate) mod handler;
mod specs;

// TODO: discovers should get a similar treatment as publications...
// - Create `pub async fn discover(discover_args: ...) -> Result<tables::DraftCatalog>`
// - call that function from the handler
// - create a `ControlPlane::discover` function for use by controllers

/// Represents the desire to discover an endpoint. The discovered bindings will be merged with
/// those in the `base_model`.
pub struct Discover {
    pub capture_name: models::Capture,
    pub data_plane_name: String,
    pub logs_token: Uuid,
    pub user_id: Uuid,
    // TODO: This _should_ agree with the `autoDiscover.addNewBindings` from the `base_model`, but it is not required to.
    pub update_only: bool,
    pub draft: tables::DraftCatalog,
}

pub type ResourcePath = Vec<String>;
pub type Changes = BTreeMap<ResourcePath, models::Collection>;

#[derive(Debug)]
pub struct DiscoverOutput {
    pub capture_name: models::Capture,
    pub draft: tables::DraftCatalog,
    pub added: Changes,
    pub modified: Changes,
    pub removed: Changes,
}

impl DiscoverOutput {
    fn failed(capture_name: models::Capture, error: anyhow::Error) -> DiscoverOutput {
        let mut draft = tables::DraftCatalog::default();
        draft.errors.insert(tables::Error {
            scope: tables::synthetic_scope(models::CatalogType::Capture, &capture_name),
            error,
        });
        DiscoverOutput {
            capture_name,
            draft,
            added: Default::default(),
            modified: Default::default(),
            removed: Default::default(),
        }
    }

    pub fn is_success(&self) -> bool {
        self.draft.errors.is_empty()
    }

    pub fn is_unchanged(&self) -> bool {
        self.added.is_empty() && self.modified.is_empty() && self.removed.is_empty()
    }

    pub fn prune_unchanged_specs(&mut self) -> usize {
        assert!(
            self.draft.errors.is_empty(),
            "cannot prune_unchanged on discover output with errors"
        );

        let mut pruned_count = 0;
        if self.is_unchanged() {
            // We've discovered absolutely no changes, so remove everything from
            // the draft. Note that this will also remove any pre-existing
            // unrelated specs.
            pruned_count = self.draft.spec_count();
            self.draft = tables::DraftCatalog::default();
        } else {
            let DiscoverOutput {
                ref mut draft,
                ref added,
                ref modified,
                ..
            } = self;
            // At least one binding has changed, so the capture spec itself must
            // be changed, and we'll only remove collection specs that have not
            // been modified. Start by determining the set of modified
            // collection names. Note that removed bindings are not included here
            // because we don't delete their collection specs so there's no need
            // to ever keep them in the draft.
            let changed_collections = added
                .values()
                .chain(modified.values())
                .collect::<HashSet<&models::Collection>>();

            draft.collections.retain(|row| {
                let retain = changed_collections.contains(&row.collection);
                if !retain {
                    pruned_count += 1;
                }
                retain
            });
        }
        pruned_count
    }
}

// - Load draft specs
// - Discover
// - Merge
//     - Load live specs
//     - Each binding:
//        - Merge and return if changed
//        - Set is_touch based on changed
//        - Add resource path to changes if so

/// A DiscoverHandler is a Handler which performs discovery operations.
pub struct DiscoverHandler {
    logs_tx: logs::Tx,
}

impl DiscoverHandler {
    pub fn new(logs_tx: &logs::Tx) -> Self {
        Self {
            logs_tx: logs_tx.clone(),
        }
    }
}

impl DiscoverHandler {
    pub async fn discover(&mut self, db: &PgPool, req: Discover) -> anyhow::Result<DiscoverOutput> {
        let Discover {
            capture_name,
            data_plane_name,
            logs_token,
            user_id,
            update_only,
            mut draft,
        } = req;

        let mut data_planes: tables::DataPlanes = agent_sql::data_plane::fetch_data_planes(
            db,
            Vec::new(),
            data_plane_name.as_str(),
            user_id,
        )
        .await?;

        let Some(data_plane) = data_planes.pop().filter(|d| d.is_default) else {
            return Ok(DiscoverOutput::failed(capture_name, anyhow::anyhow!("data-plane {} could not be resolved. It may not exist or you may not be authorized", data_plane_name)));
        };
        let Some(capture_def) = draft.captures.get_mut_by_key(&capture_name) else {
            return Ok(DiscoverOutput::failed(
                capture_name.clone(),
                anyhow::anyhow!("missing capture: '{capture_name}' in draft"),
            ));
        };

        let Some(models::CaptureEndpoint::Connector(connector_cfg)) =
            capture_def.model.as_ref().map(|m| &m.endpoint)
        else {
            anyhow::bail!("only connector endpoints are supported");
        };

        // INFO is a good default since these are not shown in the UI, so if we're looking then
        // there's already a problem.
        let log_level = capture_def
            .model
            .as_ref()
            .and_then(|m| m.shards.log_level.as_deref())
            .and_then(ops::LogLevel::from_str_name)
            .unwrap_or(ops::LogLevel::Info);

        let (image_name, image_tag) = split_image_tag(&connector_cfg.image);
        let resource_path_pointers =
            agent_sql::connector_tags::fetch_resource_path_pointers(&image_name, &image_tag, db)
                .await?;
        if resource_path_pointers.is_empty() {
            return Ok(DiscoverOutput::failed(capture_name, anyhow::anyhow!("there are no configured resource_path_pointers for connector '{}', cannot discover", connector_cfg.image)));
        }

        let config_json = serde_json::to_string(connector_cfg).unwrap();
        let request = capture::Request {
            discover: Some(capture::request::Discover {
                connector_type: capture_spec::ConnectorType::Image as i32,
                config_json,
            }),
            ..Default::default()
        }
        .with_internal(|internal| {
            internal.set_log_level(log_level);
        });

        let task = ops::ShardRef {
            name: capture_name.as_str().to_owned(),
            kind: ops::TaskType::Capture as i32,
            ..Default::default()
        };

        let log_handler =
            logs::ops_handler(self.logs_tx.clone(), "discover".to_string(), logs_token);

        let result = crate::ProxyConnectors::new(log_handler)
            .unary_capture(&data_plane, task, request)
            .await;

        let response = match result {
            Ok(response) => response,
            Err(err) => {
                return Ok(DiscoverOutput::failed(capture_name, err));
            }
        };

        Self::new_build_merged_catalog(
            capture_name,
            user_id,
            update_only,
            draft,
            response,
            resource_path_pointers,
            db,
        )
        .await
    }

    async fn new_build_merged_catalog(
        capture_name: models::Capture,
        user_id: uuid::Uuid,
        update_only: bool,
        mut draft: tables::DraftCatalog,
        response: capture::Response,
        resource_path_pointers: Vec<String>,
        db: &PgPool,
    ) -> anyhow::Result<DiscoverOutput> {
        let discovered_bindings = specs::new_parse_response(response)
            .context("converting discovery response into specs")?;

        let tables::DraftCatalog {
            ref mut captures,
            ref mut collections,
            ..
        } = &mut draft;
        let model = capture_def_mut(&capture_name, captures)?;

        let pointers = resource_path_pointers
            .iter()
            .map(|p| doc::Pointer::from_str(p.as_str()))
            .collect::<Vec<_>>();
        let (used_bindings, added_bindings, removed_bindings) = specs::update_capture_bindings(
            capture_name.as_str(),
            model,
            discovered_bindings,
            update_only,
            &pointers,
        )?;

        let collection_names = model
            .bindings
            .iter()
            .map(|b| b.target.to_string())
            .collect::<Vec<_>>();
        let live = crate::live_specs::get_live_specs(
            user_id,
            &collection_names,
            Some(models::Capability::Read),
            db,
        )
        .await?;

        let mut modified_bindings =
            specs::new_merge_collections(used_bindings, collections, &live.collections)?;
        // Don't report a binding as both added and modified, because that'd just be confusing
        modified_bindings.retain(|path, _| !added_bindings.contains_key(path));

        Ok(DiscoverOutput {
            capture_name,
            draft,
            added: added_bindings,
            modified: modified_bindings,
            removed: removed_bindings,
        })
    }
}

fn capture_def_mut<'a, 'b>(
    capture_name: &'a models::Capture,
    draft: &'b mut tables::DraftCaptures,
) -> anyhow::Result<&'b mut models::CaptureDef> {
    let Some(drafted) = draft.get_mut_by_key(capture_name) else {
        anyhow::bail!("expected capture '{}' to exist in draft", capture_name);
    };
    let Some(model) = drafted.model.as_mut() else {
        anyhow::bail!(
            "expected model to be drafted for capture '{}', but was a deletion",
            capture_name
        );
    };
    Ok(model)
}

#[cfg(test)]
mod test {

    use models::Id;
    use proto_flow::capture;
    use serde_json::json;
    use sqlx::Connection;
    use std::str::FromStr;
    use uuid::Uuid;

    const FIXED_DATABASE_URL: &str = "postgresql://postgres:postgres@localhost:5432/postgres";

    #[tokio::test]
    async fn test_catalog_merge_ok() {
        let mut conn = sqlx::postgres::PgConnection::connect(&FIXED_DATABASE_URL)
            .await
            .unwrap();
        let mut txn = conn.begin().await.unwrap();

        sqlx::query(
            r#"
            with
            p1 as (
                insert into user_grants(user_id, object_role, capability) values
                ('11111111-1111-1111-1111-111111111111', 'aliceCo/', 'admin')
            ),
            p2 as (
                insert into drafts (id, user_id) values
                ('dddddddddddddddd', '11111111-1111-1111-1111-111111111111')
            ),
            p3 as (
                insert into live_specs (catalog_name, spec_type, spec) values
                -- Existing collection which is deeply merged.
                ('aliceCo/existing-collection', 'collection', '{
                    "key": ["/old/key"],
                    "writeSchema": false,
                    "readSchema": {"const": "read!"}
                }')
            ),
            p4 as (
                insert into draft_specs (draft_id, catalog_name, spec_type, spec) values
                -- Capture which is deeply merged (modified resource config and `interval` are preserved).
                ('dddddddddddddddd', 'aliceCo/dir/source-thingy', 'capture', '{
                    "bindings": [
                        { "resource": { "table": "foo", "modified": 1 }, "target": "aliceCo/existing-collection" }
                    ],
                    "endpoint": { "connector": { "config": { "fetched": 1 }, "image": "old/image" } },
                    "interval": "10m"
                }'),
                -- Drafted collection which isn't (yet) linked to the capture, but collides
                -- with a binding being added. Expect `projections` are preserved in the merge.
                ('dddddddddddddddd', 'aliceCo/dir/quz', 'collection', '{
                    "key": ["/old/key"],
                    "schema": false,
                    "projections": {"a-field": "/some/ptr"}
                }')
            )
            select 1;
            "#,
        )
        .execute(&mut txn)
        .await
        .unwrap();

        let response : capture::Response = serde_json::from_value(json!({
            "discovered": {
                "bindings": [
                    {"documentSchema": {"const": "write!"}, "key": ["/foo"], "recommendedName": "foo", "resourceConfig": {"table": "foo"}},
                    {"documentSchema": {"const": "bar"}, "key": ["/bar"], "recommendedName": "bar", "resourceConfig": {"table": "bar"}},
                    {"documentSchema": {"const": "quz"}, "key": ["/quz"], "recommendedName": "quz", "resourceConfig": {"table": "quz"}},
                ],
            }
        })).unwrap();

        let endpoint_config =
            serde_json::value::to_raw_value(&json!({"some": "endpoint-config"})).unwrap();

        let result = super::DiscoverHandler::build_merged_catalog(
            "aliceCo/dir/source-thingy",
            response,
            Id::from_hex("dddddddddddddddd").unwrap(),
            &endpoint_config,
            "ghcr.io/estuary/source-thingy",
            ":v1",
            false,
            false,
            Uuid::from_str("11111111-1111-1111-1111-111111111111").unwrap(),
            &mut txn,
        )
        .await;

        let catalog = result.unwrap().unwrap();
        insta::assert_json_snapshot!(json!(catalog));
    }

    #[tokio::test]
    async fn test_catalog_merge_endpoint_changed() {
        let mut conn = sqlx::postgres::PgConnection::connect(&FIXED_DATABASE_URL)
            .await
            .unwrap();
        let mut txn = conn.begin().await.unwrap();

        sqlx::query(
            r#"
            with
            p1 as (
                insert into user_grants(user_id, object_role, capability) values
                ('11111111-1111-1111-1111-111111111111', 'aliceCo/', 'admin')
            ),
            p2 as (
                insert into drafts (id, user_id) values
                ('eeeeeeeeeeeeeeee', '11111111-1111-1111-1111-111111111111')
            ),
            p3 as (
                insert into live_specs (catalog_name, spec_type, spec) values
                -- Existing capture which is deeply merged.
                ('aliceCo/dir/source-thingy', 'capture', '{
                    "bindings": [ ],
                    "endpoint": { "connector": { "config": { "a": "oldA" }, "image": "an/image" } },
                    "interval": "10m"
                }')
            )
            select 1;
            "#,
        )
        .execute(&mut txn)
        .await
        .unwrap();

        let response : capture::Response = serde_json::from_value(json!({
            "discovered": {
                "bindings": [
                    {"documentSchema": {"const": "write!"}, "key": ["/foo"], "recommendedName": "foo", "resourceConfig": {"table": "foo"}},
                    {"documentSchema": {"const": "bar"}, "key": ["/bar"], "recommendedName": "bar", "resourceConfig": {"table": "bar"}},
                    {"documentSchema": {"const": "quz"}, "key": ["/quz"], "recommendedName": "quz", "resourceConfig": {"table": "quz"}},
                ],
            }
        })).unwrap();

        let endpoint_config = serde_json::value::to_raw_value(&json!({"a": "newA"})).unwrap();

        let result = super::DiscoverHandler::build_merged_catalog(
            "aliceCo/dir/source-thingy",
            response,
            Id::from_hex("eeeeeeeeeeeeeeee").unwrap(),
            &endpoint_config,
            "ghcr.io/estuary/source-thingy",
            ":v1",
            false,
            true,
            Uuid::from_str("11111111-1111-1111-1111-111111111111").unwrap(),
            &mut txn,
        )
        .await;

        let errs = result
            .unwrap()
            .expect_err("expected inner result to be an error");
        assert_eq!(1, errs.len());
        assert_eq!(
            "capture endpoint has been modified since the discover was created (will retry)",
            &errs[0].detail
        );
    }

    #[tokio::test]
    async fn test_catalog_merge_bad_spec() {
        let mut conn = sqlx::postgres::PgConnection::connect(&FIXED_DATABASE_URL)
            .await
            .unwrap();
        let mut txn = conn.begin().await.unwrap();

        sqlx::query(
            r#"
            with
            p1 as (
                insert into drafts (id, user_id) values
                ('dddddddddddddddd', '11111111-1111-1111-1111-111111111111')
            ),
            p2 as (
                insert into draft_specs (draft_id, catalog_name, spec_type, spec) values
                ('dddddddddddddddd', 'aliceCo/bad', 'collection', '{"key": "invalid"}')
            )
            select 1;
            "#,
        )
        .execute(&mut txn)
        .await
        .unwrap();

        let response : capture::Response = serde_json::from_value(json!({
            "discovered": {
                "bindings": [
                    {"documentSchema": {"const": 42}, "key": ["/key"], "recommendedName": "bad", "resourceConfig": {"table": "bad"}},
                ],
            }
        })).unwrap();

        let result = super::DiscoverHandler::build_merged_catalog(
            "aliceCo/source-thingy",
            response,
            Id::from_hex("dddddddddddddddd").unwrap(),
            &serde_json::value::to_raw_value(&json!({"some": "endpoint-config"})).unwrap(),
            "ghcr.io/estuary/source-thingy",
            ":v1",
            false,
            false,
            Uuid::from_str("11111111-1111-1111-1111-111111111111").unwrap(),
            &mut txn,
        )
        .await;

        let errors = result.unwrap().unwrap_err();
        insta::assert_debug_snapshot!(errors, @r###"
        [
            Error {
                catalog_name: "aliceCo/bad",
                scope: None,
                detail: "parsing collection aliceCo/bad: invalid type: string \"invalid\", expected a sequence at line 1 column 17",
            },
        ]
        "###);
    }
}
