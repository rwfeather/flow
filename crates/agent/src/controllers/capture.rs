use std::{collections::BTreeMap, time::Duration};

use super::{
    backoff_data_plane_activate,
    publication_status::{ActivationStatus, PendingPublication},
    ControlPlane, ControllerErrorExt, ControllerState, NextRun,
};
use crate::discovers::{Changes, Discover};
use crate::{controllers::publication_status::PublicationStatus, publications};
use anyhow::Context;
use chrono::{DateTime, Utc};
use itertools::Itertools;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Status of a capture controller
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, JsonSchema)]
pub struct CaptureStatus {
    // TODO: auto discovers are not yet implemented as controllers, but they should be soon.
    // #[serde(default, skip_serializing_if = "Option::is_none")]
    // #[schemars(schema_with = "super::datetime_schema")]
    // pub next_auto_discover: Option<DateTime<Utc>>,
    #[serde(default)]
    pub publications: PublicationStatus,
    #[serde(default)]
    pub activation: ActivationStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auto_discover: Option<AutoDiscoverStatus>,
}

impl CaptureStatus {
    pub async fn update<C: ControlPlane>(
        &mut self,
        state: &ControllerState,
        control_plane: &mut C,
        model: &models::CaptureDef,
    ) -> anyhow::Result<Option<NextRun>> {
        let mut pending_pub = PendingPublication::new();
        let dependencies = self
            .publications
            .resolve_dependencies(state, control_plane)
            .await?;
        if dependencies.hash != state.live_dependency_hash {
            if dependencies.deleted.is_empty() {
                pending_pub.start_touch(state, dependencies.hash.as_deref());
            } else {
                let draft = pending_pub.start_spec_update(
                    state,
                    format!("in response to publication of one or more depencencies"),
                );
                tracing::debug!(deleted_collections = ?dependencies.deleted, "disabling bindings for collections that have been deleted");
                let draft_capture = draft
                    .captures
                    .get_mut_by_key(&models::Capture::new(&state.catalog_name))
                    .expect("draft must contain capture");
                let mut disabled_count = 0;
                for binding in draft_capture.model.as_mut().unwrap().bindings.iter_mut() {
                    if dependencies.deleted.contains(binding.target.as_str()) && !binding.disable {
                        disabled_count += 1;
                        binding.disable = true;
                    }
                }
                let detail = format!(
                    "disabled {disabled_count} binding(s) in response to deleted collections: [{}]",
                    dependencies.deleted.iter().format(", ")
                );
                pending_pub.update_pending_draft(detail);
            }
        }

        let ad_next_run = if model.auto_discover.is_some() {
            let ad_status = self.auto_discover.get_or_insert_with(Default::default);
            let next_auto_discover = ad_status
                .update(state, control_plane, model, &mut pending_pub)
                .await
                .context("updating auto-discover")?;
            Some(next_auto_discover)
        } else {
            None
        };

        if pending_pub.has_pending() {
            let _result = pending_pub
                .finish(state, &mut self.publications, control_plane)
                .await
                .context("failed to execute publish")?
                .error_for_status()
                .with_maybe_retry(backoff_publication_failure(state.failures))?;
        } else {
            // Not much point in activating if we just published, since we're going to be
            // immediately invoked again.
            self.activation
                .update(state, control_plane)
                .await
                .with_retry(backoff_data_plane_activate(state.failures))?;
            self.publications
                .notify_dependents(state, control_plane)
                .await
                .context("failed to notify dependents")?;
        }

        Ok(None)
    }
}

pub type ReCreatedCollections = BTreeMap<models::Collection, models::Collection>;

#[derive(Debug, Serialize, Deserialize, PartialEq, JsonSchema, Clone)]
pub struct AutoDiscoverOutcome {
    #[schemars(schema_with = "super::datetime_schema")]
    pub ts: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Changes::is_empty")]
    pub added: Changes,
    #[serde(default, skip_serializing_if = "Changes::is_empty")]
    pub modified: Changes,
    #[serde(default, skip_serializing_if = "Changes::is_empty")]
    pub removed: Changes,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub errors: Vec<crate::draft::Error>,
    #[serde(default, skip_serializing_if = "ReCreatedCollections::is_empty")]
    pub re_created_collections: ReCreatedCollections,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publish_result: Option<publications::JobStatus>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, JsonSchema, Clone)]
pub struct AutoDiscoverFailure {
    pub count: usize,
    #[schemars(schema_with = "super::datetime_schema")]
    pub first_ts: DateTime<Utc>,
    pub last_oucome: AutoDiscoverOutcome,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, JsonSchema)]
pub struct AutoDiscoverStatus {
    #[serde(default, with = "humantime_serde")]
    #[schemars(schema_with = "interval_schema")]
    pub interval: Option<Duration>,
    pub pending_publish: Option<AutoDiscoverOutcome>,
    pub last_success: Option<AutoDiscoverOutcome>,
    pub failure: Option<AutoDiscoverFailure>,
}

fn interval_schema(_: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
    serde_json::from_value(serde_json::json!({
        "type": ["string", "null"],
        "pattern": "^\\d+(s|m|h)$"
    }))
    .unwrap()
}

impl AutoDiscoverStatus {
    /// Default to running auto-discovers every 2 hours
    const DEFAULT_INTERVAL: Duration = Duration::from_secs(60 * 60 * 2);

    async fn update<C: ControlPlane>(
        &mut self,
        state: &ControllerState,
        control_plane: &mut C,
        model: &models::CaptureDef,
        pending: &mut PendingPublication,
    ) -> anyhow::Result<NextRun> {
        let next_disco_time = self.next_discover_time(state);
        if control_plane.current_time() <= next_disco_time {
            return Ok(NextRun::at(next_disco_time));
        }

        todo!()
    }

    fn next_discover_time(&self, state: &ControllerState) -> DateTime<Utc> {
        if let Some(failure) = self.failure.as_ref() {
            let backoff_minutes = match failure.count {
                0 => 0, // just in case someone manually sets the failure count to 0
                1 => 15,
                n @ 2..=4 => n * 30,
                n @ 5.. => n.min(23) * 60,
            };
            failure.last_oucome.ts + chrono::Duration::minutes(backoff_minutes)
        } else {
            let last_disco_time = self
                .last_success
                .as_ref()
                .map(|s| s.ts)
                .unwrap_or(state.created_at);
            let interval = self.interval.unwrap_or(Self::DEFAULT_INTERVAL);
            last_disco_time + interval
        }
    }

    // async fn publication_finished()
}

fn backoff_publication_failure(prev_failures: i32) -> Option<NextRun> {
    if prev_failures < 3 {
        Some(NextRun::after_minutes(prev_failures.max(1) as u32))
    } else if prev_failures < 10 {
        Some(NextRun::after_minutes(prev_failures as u32 * 60))
    } else {
        None
    }
}
