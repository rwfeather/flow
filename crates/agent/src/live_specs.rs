use anyhow::Context;
use models::Capability;
use std::ops::Deref;
use uuid::Uuid;

/// Fetches live specs, returning them as a `tables::LiveCatalog`. Optionally
/// filters the specs based on user capability. If `filter_capability` is
/// `None`, then no filtering will be done.
pub async fn get_live_specs(
    user_id: Uuid,
    names: &[String],
    filter_capability: Option<Capability>,
    db: &sqlx::PgPool,
) -> anyhow::Result<tables::LiveCatalog> {
    let rows = agent_sql::live_specs::fetch_live_specs(user_id, &names, db).await?;
    let mut live = tables::LiveCatalog::default();
    for row in rows {
        // Spec type might be null because we used to set it to null when deleting specs.
        // For recently deleted specs, it will still be present.
        let Some(catalog_type) = row.spec_type.map(Into::into) else {
            continue;
        };
        let Some(model_json) = row.spec.as_deref() else {
            continue;
        };
        if let Some(min_capability) = filter_capability {
            if !row
                .user_capability
                .is_some_and(|actual_capability| actual_capability >= min_capability)
            {
                continue;
            }
        }
        let built_spec_json = row.built_spec.as_ref().ok_or_else(|| {
            tracing::warn!(catalog_name = %row.catalog_name, id = %row.id, "got row with spec but not built_spec");
            anyhow::anyhow!("missing built_spec for {:?}, but spec is non-null", row.catalog_name)
        })?.deref();

        live.add_spec(
            catalog_type,
            &row.catalog_name,
            row.id.into(),
            row.data_plane_id.into(),
            row.last_pub_id.into(),
            row.last_build_id.into(),
            model_json,
            built_spec_json,
            row.dependency_hash,
        )
        .with_context(|| format!("deserializing specs for {:?}", row.catalog_name))?;
    }

    Ok(live)
}
