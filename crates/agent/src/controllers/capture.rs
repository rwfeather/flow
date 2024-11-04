use std::{collections::BTreeMap, time::Duration};

use super::{
    backoff_data_plane_activate,
    publication_status::{ActivationStatus, PendingPublication},
    ControlPlane, ControllerErrorExt, ControllerState, NextRun,
};
use crate::{
    controllers::publication_status::PublicationStatus,
    discovers::{Changed, ResourcePath},
    publications,
};
use crate::{
    discovers::{Changes, Discover, DiscoverOutput},
    publications::PublicationResult,
};
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
            let mut pub_result = pending_pub
                .finish(state, &mut self.publications, control_plane)
                .await
                .context("failed to execute publish")?;

            if let Some(auto_discovers) = self.auto_discover.as_mut() {
                pub_result = auto_discovers
                    .publication_finished(pub_result, state, control_plane, model)
                    .await?;
            }

            pub_result
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

        Ok(ad_next_run)
    }
}

pub type ReCreatedCollections = BTreeMap<models::Collection, models::Collection>;

#[derive(Debug, Serialize, Deserialize, PartialEq, JsonSchema, Clone)]
pub struct AutoDiscoverOutcome {
    #[schemars(schema_with = "super::datetime_schema")]
    pub ts: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub added: Vec<(ResourcePath, models::Collection)>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub modified: Vec<(ResourcePath, models::Collection)>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub removed: Vec<(ResourcePath, models::Collection)>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub errors: Vec<crate::draft::Error>,
    #[serde(default, skip_serializing_if = "ReCreatedCollections::is_empty")]
    pub re_created_collections: ReCreatedCollections,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publish_result: Option<publications::JobStatus>,
}

impl AutoDiscoverOutcome {
    fn from_output(ts: DateTime<Utc>, output: DiscoverOutput) -> (Self, tables::DraftCatalog) {
        let DiscoverOutput {
            capture_name: _,
            draft,
            added,
            modified,
            removed,
        } = output;

        let errors = draft
            .errors
            .iter()
            .map(crate::draft::Error::from_tables_error)
            .collect();

        let outcome = Self {
            ts,
            added: added
                .into_iter()
                .map(|(rp, change)| (rp, change.target))
                .collect(),
            modified: modified
                .into_iter()
                .map(|(rp, change)| (rp, change.target))
                .collect(),
            removed: removed.into_iter().collect(),
            errors,
            re_created_collections: Default::default(),
            publish_result: None,
        };
        (outcome, draft)
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, JsonSchema, Clone)]
pub struct AutoDiscoverFailure {
    /// The number of consecutive failures that have been observed.
    pub count: u32,
    /// The timestamp of the first failure in the current sequence.
    #[schemars(schema_with = "super::datetime_schema")]
    pub first_ts: DateTime<Utc>,
    /// The discover outcome corresponding to the most recent failure. This will
    /// be updated with the results of each retry until an auto-discover
    /// succeeds.
    pub last_outcome: AutoDiscoverOutcome,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, JsonSchema)]
pub struct AutoDiscoverStatus {
    /// The interval at which auto-discovery is run. This is normally unset, which uses
    /// the default interval.
    #[serde(
        default,
        with = "humantime_serde",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(schema_with = "interval_schema")]
    pub interval: Option<Duration>,
    /// The outcome of the a recent discover, which is about to be published.
    /// This will typically only be observed if the publication failed for some
    /// reason.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_publish: Option<AutoDiscoverOutcome>,
    /// The outcome of the last _successful_ auto-discover. If `failure` is set,
    /// then that will typically be more recent than `last_success`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_success: Option<AutoDiscoverOutcome>,
    /// If auto-discovery has failed, this will include information about that failure.
    /// This field is cleared as soon as a successful auto-discover is run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
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
            return Ok(NextRun::after(next_disco_time));
        }
        // Time to discover. Start by clearing out any pending publish, since we'll use the outcome
        // of the discover to determine that.
        self.pending_publish = None;
        let update_only = !model.auto_discover.as_ref().unwrap().add_new_bindings;
        let capture_name = models::Capture::new(&state.catalog_name);

        let mut draft = std::mem::take(&mut pending.draft);
        if !draft.captures.get_by_key(&capture_name).is_some() {
            draft.captures.insert(tables::DraftCapture {
                capture: capture_name.clone(),
                scope: tables::synthetic_scope(models::CatalogType::Capture, &capture_name),
                expect_pub_id: Some(state.last_pub_id),
                model: Some(model.clone()),
                // start with a touch. The discover merge will set this to false if it actually updates the capture
                is_touch: true,
            });
        }

        let mut output = control_plane
            .discover(
                models::Capture::new(&state.catalog_name),
                draft,
                update_only,
                state.logs_token,
                state.data_plane_id,
            )
            .await
            .context("failed to discover")?;

        // Return early if there was a discover error.
        if !output.is_success() {
            let (outcome, _) =
                AutoDiscoverOutcome::from_output(control_plane.current_time(), output);
            if let Some(failure) = self.failure.as_mut() {
                failure.count += 1;
                failure.last_outcome = outcome;
            } else {
                self.failure = Some(AutoDiscoverFailure {
                    count: 1,
                    first_ts: control_plane.current_time(),
                    last_outcome: outcome,
                });
            }
            let retry_at = self.next_discover_time(state);
            return Ok(NextRun::after(retry_at));
        }

        // The discover was successful, but has anything actually changed?
        // First prune the discovered draft to remove any unchanged specs.
        let unchanged_count = output.prune_unchanged_specs();
        if output.is_unchanged() {
            let (outcome, _) =
                AutoDiscoverOutcome::from_output(control_plane.current_time(), output);
            self.failure = None; // Clear any previous failure.
            self.last_success = Some(outcome);
            return Ok(NextRun::after(self.next_discover_time(state)));
        }

        let (outcome, draft) =
            AutoDiscoverOutcome::from_output(control_plane.current_time(), output);

        debug_assert!(
            draft.spec_count() > 0,
            "draft should have at least one spec since is_unchanged() returned false"
        );

        // Try to publish the changes
        tracing::info!(
            %unchanged_count,
            drafted_count = %draft.spec_count(),
            added = %outcome.added.len(),
            modified = %outcome.modified.len(),
            removed = %outcome.removed.len(),
            "Auto-discover has changes to publish"
        );

        let publish_detail = format!(
            "auto-discover changes ({} added, {} modified, {} removed)",
            outcome.added.len(),
            outcome.modified.len(),
            outcome.removed.len(),
        );
        pending.details.push(publish_detail);
        // Add the draft back into the pending publication, so it will be published.
        pending.draft = draft;

        self.pending_publish = Some(outcome);

        Ok(NextRun::after(self.next_discover_time(state)))
    }

    async fn publication_finished<C: ControlPlane>(
        &mut self,
        mut result: PublicationResult,
        state: &ControllerState,
        control_plane: &mut C,
        model: &models::CaptureDef,
    ) -> anyhow::Result<PublicationResult> {
        let Some(mut pending_outcome) = self.pending_publish.take() else {
            // Nothing to do if we didn't attempt to publish. This just means that the publication
            // was due to dependency updates, not auto-discover.
            return Ok(result);
        };

        // Did the publication result in incompatible collections, which we should evolve?
        let evolve_incompatible = model
            .auto_discover
            .as_ref()
            .unwrap()
            .evolve_incompatible_collections;
        if evolve_incompatible && result.status.has_incompatible_collections() {
            tracing::error!("oh no incompatible collections!");
            anyhow::bail!("incompatible collections, oh nooooo");
            // todo!("implement evolve incompatible collections and retry publish");
        }

        pending_outcome.publish_result = Some(result.status.clone());

        if result.status.is_success() || result.status.is_empty_draft() {
            self.failure = None;
            self.last_success = Some(pending_outcome);
        } else {
            if let Some(fail) = self.failure.as_mut() {
                fail.count += 1;
                fail.last_outcome = pending_outcome;
            } else {
                self.failure = Some(AutoDiscoverFailure {
                    count: 1,
                    first_ts: pending_outcome.ts,
                    last_outcome: pending_outcome,
                });
            }
        }

        return Ok(result);
    }

    fn next_discover_time(&self, state: &ControllerState) -> DateTime<Utc> {
        let interval = self.interval.unwrap_or(Self::DEFAULT_INTERVAL);
        if let Some(failure) = self.failure.as_ref() {
            // We scale the backoff multiplier based on the configured interval
            // here. This is both to keep the backoffs reasonable, and to allow
            // us to test multiple failure scenarios in integration tests.
            let backoff_secs = match failure.count as u64 {
                0 => 0, // just in case someone manually sets the failure count to 0
                1 => interval.as_secs() / 8,
                n @ 2..=4 => n * (interval.as_secs() / 4),
                n @ 5.. => n.min(23) * (interval.as_secs() / 2),
            };
            tracing::info!( %backoff_secs, "Auto-discover will retry after backoff");
            failure.last_outcome.ts + chrono::Duration::seconds(backoff_secs as i64)
        } else {
            let last_disco_time = self
                .pending_publish
                .as_ref()
                .map(|s| s.ts)
                .or_else(|| self.last_success.as_ref().map(|p| p.ts))
                .unwrap_or(state.created_at);

            let next = last_disco_time + interval;
            tracing::info!(%last_disco_time, ?interval, %next, "determined next auto-discover run time");
            next
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
