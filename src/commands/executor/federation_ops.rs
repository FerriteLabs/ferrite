//! Federation and multi-region active-active command implementations on CommandExecutor.

use std::sync::OnceLock;

use crate::protocol::Frame;

use super::CommandExecutor;

/// Global active-active replicator instance (lazily initialized).
#[cfg(feature = "experimental")]
fn active_active_replicator() -> &'static ferrite_enterprise::active_active::ActiveActiveReplicator
{
    static REPLICATOR: OnceLock<ferrite_enterprise::active_active::ActiveActiveReplicator> =
        OnceLock::new();
    REPLICATOR.get_or_init(|| {
        ferrite_enterprise::active_active::ActiveActiveReplicator::with_defaults(
            "local".to_string(),
        )
    })
}

/// Global Data Mesh Gateway instance (lazily initialized).
#[cfg(feature = "experimental")]
fn mesh_gateway() -> &'static ferrite_enterprise::mesh::DataMeshGateway {
    static GATEWAY: OnceLock<ferrite_enterprise::mesh::DataMeshGateway> = OnceLock::new();
    GATEWAY.get_or_init(ferrite_enterprise::mesh::DataMeshGateway::with_defaults)
}

impl CommandExecutor {
    // ── Multi-region active-active handlers ───────────────────────────

    #[cfg(feature = "experimental")]
    pub(super) async fn handle_region_add(&self, id: &str, name: &str, endpoint: &str) -> Frame {
        let replicator = active_active_replicator();
        match replicator.add_region(id.to_string(), name.to_string(), endpoint.to_string()) {
            Ok(()) => Frame::simple("OK"),
            Err(e) => Frame::error(format!("ERR {e}")),
        }
    }

    #[cfg(not(feature = "experimental"))]
    pub(super) async fn handle_region_add(&self, _id: &str, _name: &str, _endpoint: &str) -> Frame {
        Frame::error("ERR REGION commands require the 'experimental' feature")
    }

    #[cfg(feature = "experimental")]
    pub(super) async fn handle_region_remove(&self, id: &str) -> Frame {
        let replicator = active_active_replicator();
        match replicator.remove_region(id) {
            Ok(()) => Frame::simple("OK"),
            Err(e) => Frame::error(format!("ERR {e}")),
        }
    }

    #[cfg(not(feature = "experimental"))]
    pub(super) async fn handle_region_remove(&self, _id: &str) -> Frame {
        Frame::error("ERR REGION commands require the 'experimental' feature")
    }

    #[cfg(feature = "experimental")]
    pub(super) async fn handle_region_list(&self) -> Frame {
        let replicator = active_active_replicator();
        let regions = replicator.list_regions();
        if regions.is_empty() {
            return Frame::Array(Some(vec![]));
        }
        let items: Vec<Frame> = regions
            .into_iter()
            .map(|r| {
                let status = r.status.to_string();
                Frame::Array(Some(vec![
                    Frame::bulk("id"),
                    Frame::bulk(r.id),
                    Frame::bulk("name"),
                    Frame::bulk(r.name),
                    Frame::bulk("endpoint"),
                    Frame::bulk(r.endpoint),
                    Frame::bulk("status"),
                    Frame::bulk(status),
                    Frame::bulk("lag_ms"),
                    Frame::Integer(r.replication_lag_ms as i64),
                    Frame::bulk("keys_synced"),
                    Frame::Integer(r.keys_synced as i64),
                    Frame::bulk("conflicts_resolved"),
                    Frame::Integer(r.conflicts_resolved as i64),
                ]))
            })
            .collect();
        Frame::Array(Some(items))
    }

    #[cfg(not(feature = "experimental"))]
    pub(super) async fn handle_region_list(&self) -> Frame {
        Frame::error("ERR REGION commands require the 'experimental' feature")
    }

    #[cfg(feature = "experimental")]
    pub(super) async fn handle_region_status(&self, id: Option<&str>) -> Frame {
        let replicator = active_active_replicator();
        match id {
            Some(region_id) => {
                if let Some(r) = replicator.get_region(region_id) {
                    let status = r.status.to_string();
                    Frame::Array(Some(vec![
                        Frame::bulk("id"),
                        Frame::bulk(r.id),
                        Frame::bulk("name"),
                        Frame::bulk(r.name),
                        Frame::bulk("endpoint"),
                        Frame::bulk(r.endpoint),
                        Frame::bulk("status"),
                        Frame::bulk(status),
                        Frame::bulk("lag_ms"),
                        Frame::Integer(r.replication_lag_ms as i64),
                        Frame::bulk("keys_synced"),
                        Frame::Integer(r.keys_synced as i64),
                    ]))
                } else {
                    Frame::error(format!("ERR Region '{region_id}' not found"))
                }
            }
            None => {
                let stats = replicator.stats();
                let strategy = replicator.conflict_strategy().to_string();
                Frame::Array(Some(vec![
                    Frame::bulk("local_region"),
                    Frame::bulk(replicator.local_region().to_string()),
                    Frame::bulk("regions_active"),
                    Frame::Integer(stats.regions_active as i64),
                    Frame::bulk("strategy"),
                    Frame::bulk(strategy),
                ]))
            }
        }
    }

    #[cfg(not(feature = "experimental"))]
    pub(super) async fn handle_region_status(&self, _id: Option<&str>) -> Frame {
        Frame::error("ERR REGION commands require the 'experimental' feature")
    }

    #[cfg(feature = "experimental")]
    pub(super) async fn handle_region_conflicts(&self, limit: usize) -> Frame {
        let replicator = active_active_replicator();
        let conflicts = replicator.get_conflicts(limit);
        if conflicts.is_empty() {
            return Frame::Array(Some(vec![]));
        }
        let items: Vec<Frame> = conflicts
            .into_iter()
            .map(|c| {
                Frame::Array(Some(vec![
                    Frame::bulk("key"),
                    Frame::bulk(c.key),
                    Frame::bulk("strategy"),
                    Frame::bulk(c.strategy),
                    Frame::bulk("winner"),
                    Frame::bulk(c.winner),
                    Frame::bulk("resolved_at"),
                    Frame::bulk(c.resolved_at),
                ]))
            })
            .collect();
        Frame::Array(Some(items))
    }

    #[cfg(not(feature = "experimental"))]
    pub(super) async fn handle_region_conflicts(&self, _limit: usize) -> Frame {
        Frame::error("ERR REGION commands require the 'experimental' feature")
    }

    #[cfg(feature = "experimental")]
    pub(super) async fn handle_region_strategy(&self, strategy: Option<&str>) -> Frame {
        let replicator = active_active_replicator();
        match strategy {
            Some(s) => {
                match ferrite_enterprise::active_active::ConflictStrategy::from_str_loose(s) {
                    Some(_strat) => Frame::simple(format!("OK (strategy would be set to: {s})")),
                    None => Frame::error(format!(
                        "ERR Unknown strategy '{s}'. Use: lww, highest-region-id, merge"
                    )),
                }
            }
            None => {
                let strategy = replicator.conflict_strategy().to_string();
                Frame::bulk(strategy)
            }
        }
    }

    #[cfg(not(feature = "experimental"))]
    pub(super) async fn handle_region_strategy(&self, _strategy: Option<&str>) -> Frame {
        Frame::error("ERR REGION commands require the 'experimental' feature")
    }

    #[cfg(feature = "experimental")]
    pub(super) async fn handle_region_stats(&self) -> Frame {
        let replicator = active_active_replicator();
        let s = replicator.stats();
        Frame::Array(Some(vec![
            Frame::bulk("ops_replicated"),
            Frame::Integer(s.ops_replicated as i64),
            Frame::bulk("conflicts_detected"),
            Frame::Integer(s.conflicts_detected as i64),
            Frame::bulk("conflicts_resolved"),
            Frame::Integer(s.conflicts_resolved as i64),
            Frame::bulk("regions_active"),
            Frame::Integer(s.regions_active as i64),
            Frame::bulk("avg_lag_ms"),
            Frame::Integer(s.avg_lag_ms as i64),
        ]))
    }

    #[cfg(not(feature = "experimental"))]
    pub(super) async fn handle_region_stats(&self) -> Frame {
        Frame::error("ERR REGION commands require the 'experimental' feature")
    }

    // ---- Integrated Observability Diagnostics (FERRITE.DEBUG) ----

    #[cfg(feature = "experimental")]
    pub(super) async fn handle_federation_add(
        &self,
        id: &str,
        source_type: &str,
        uri: &str,
        name: Option<&str>,
    ) -> Frame {
        use ferrite_enterprise::mesh::datasource::{DataSourceConfig, DataSourceType};

        let Some(stype) = DataSourceType::from_str_ci(source_type) else {
            return Frame::error(format!("ERR unknown source type '{source_type}'"));
        };

        let display_name = name.unwrap_or(id).to_string();
        let config = DataSourceConfig::new(id.to_string(), display_name, stype, uri.to_string());

        let gw = mesh_gateway();
        match gw.add_source(config) {
            Ok(()) => Frame::simple("OK"),
            Err(e) => Frame::error(format!("ERR {e}")),
        }
    }

    #[cfg(not(feature = "experimental"))]
    pub(super) async fn handle_federation_add(
        &self,
        _id: &str,
        _source_type: &str,
        _uri: &str,
        _name: Option<&str>,
    ) -> Frame {
        Frame::error("ERR FEDERATION commands require the 'experimental' feature")
    }

    #[cfg(feature = "experimental")]
    pub(super) async fn handle_federation_remove(&self, id: &str) -> Frame {
        let gw = mesh_gateway();
        match gw.remove_source(id) {
            Ok(()) => Frame::simple("OK"),
            Err(e) => Frame::error(format!("ERR {e}")),
        }
    }

    #[cfg(not(feature = "experimental"))]
    pub(super) async fn handle_federation_remove(&self, _id: &str) -> Frame {
        Frame::error("ERR FEDERATION commands require the 'experimental' feature")
    }

    #[cfg(feature = "experimental")]
    pub(super) async fn handle_federation_list(&self) -> Frame {
        let gw = mesh_gateway();
        let sources = gw.list_sources();
        if sources.is_empty() {
            return Frame::array(vec![]);
        }
        let items: Vec<Frame> = sources
            .iter()
            .map(|s| {
                Frame::array(vec![
                    Frame::bulk(s.id.clone()),
                    Frame::bulk(s.source_type.to_string()),
                    Frame::bulk(s.uri.clone()),
                    Frame::bulk(s.name.clone()),
                    Frame::bulk(s.status.to_string()),
                ])
            })
            .collect();
        Frame::array(items)
    }

    #[cfg(not(feature = "experimental"))]
    pub(super) async fn handle_federation_list(&self) -> Frame {
        Frame::error("ERR FEDERATION commands require the 'experimental' feature")
    }

    #[cfg(feature = "experimental")]
    pub(super) async fn handle_federation_status(&self, id: Option<&str>) -> Frame {
        let gw = mesh_gateway();
        match id {
            Some(source_id) => match gw.health_check(source_id) {
                Ok(result) => Frame::array(vec![
                    Frame::bulk("source_id"),
                    Frame::bulk(result.source_id),
                    Frame::bulk("healthy"),
                    Frame::Integer(i64::from(result.healthy)),
                    Frame::bulk("latency_ms"),
                    Frame::Integer(result.latency_ms as i64),
                    Frame::bulk("message"),
                    Frame::bulk(result.message),
                ]),
                Err(e) => Frame::error(format!("ERR {e}")),
            },
            None => {
                let stats = gw.stats();
                Frame::array(vec![
                    Frame::bulk("sources_active"),
                    Frame::Integer(stats.sources_active as i64),
                    Frame::bulk("namespaces"),
                    Frame::Integer(stats.namespaces_registered as i64),
                    Frame::bulk("contracts"),
                    Frame::Integer(stats.contracts_active as i64),
                ])
            }
        }
    }

    #[cfg(not(feature = "experimental"))]
    pub(super) async fn handle_federation_status(&self, _id: Option<&str>) -> Frame {
        Frame::error("ERR FEDERATION commands require the 'experimental' feature")
    }

    #[cfg(feature = "experimental")]
    pub(super) async fn handle_federation_namespace(
        &self,
        namespace: &str,
        source_id: &str,
    ) -> Frame {
        let gw = mesh_gateway();
        match gw.add_namespace(namespace, source_id) {
            Ok(()) => Frame::simple("OK"),
            Err(e) => Frame::error(format!("ERR {e}")),
        }
    }

    #[cfg(not(feature = "experimental"))]
    pub(super) async fn handle_federation_namespace(
        &self,
        _namespace: &str,
        _source_id: &str,
    ) -> Frame {
        Frame::error("ERR FEDERATION commands require the 'experimental' feature")
    }

    #[cfg(feature = "experimental")]
    pub(super) async fn handle_federation_namespaces(&self) -> Frame {
        let gw = mesh_gateway();
        let ns = gw.list_namespaces();
        if ns.is_empty() {
            return Frame::array(vec![]);
        }
        let items: Vec<Frame> = ns
            .iter()
            .map(|(namespace, source_id)| {
                Frame::array(vec![
                    Frame::bulk(namespace.clone()),
                    Frame::bulk(source_id.clone()),
                ])
            })
            .collect();
        Frame::array(items)
    }

    #[cfg(not(feature = "experimental"))]
    pub(super) async fn handle_federation_namespaces(&self) -> Frame {
        Frame::error("ERR FEDERATION commands require the 'experimental' feature")
    }

    #[cfg(feature = "experimental")]
    pub(super) async fn handle_federation_query(&self, query: &str) -> Frame {
        use ferrite_enterprise::mesh::query_router::QueryRouter;
        use std::sync::Arc;

        let router = QueryRouter::new(Arc::new(
            ferrite_enterprise::mesh::DataMeshGateway::with_defaults(),
        ));
        match router.route_query(query) {
            Ok(plan) => {
                let steps: Vec<Frame> = plan
                    .steps
                    .iter()
                    .map(|s| {
                        Frame::array(vec![
                            Frame::bulk(s.source_id.clone()),
                            Frame::bulk(s.step_type.to_string()),
                            Frame::bulk(s.query.clone()),
                        ])
                    })
                    .collect();
                Frame::array(vec![
                    Frame::bulk("steps"),
                    Frame::array(steps),
                    Frame::bulk("estimated_latency_ms"),
                    Frame::Integer(plan.estimated_latency_ms as i64),
                    Frame::bulk("sources_involved"),
                    Frame::array(
                        plan.sources_involved
                            .iter()
                            .map(|s| Frame::bulk(s.clone()))
                            .collect(),
                    ),
                ])
            }
            Err(e) => {
                mesh_gateway().record_error();
                Frame::error(format!("ERR {e}"))
            }
        }
    }

    #[cfg(not(feature = "experimental"))]
    pub(super) async fn handle_federation_query(&self, _query: &str) -> Frame {
        Frame::error("ERR FEDERATION commands require the 'experimental' feature")
    }

    #[cfg(feature = "experimental")]
    pub(super) async fn handle_federation_contract(
        &self,
        name: &str,
        source_id: &str,
        schema_json: &str,
    ) -> Frame {
        use ferrite_enterprise::mesh::contract::DataContract;
        use ferrite_enterprise::mesh::datasource::DataSchema;

        let schema: DataSchema = match serde_json::from_str(schema_json) {
            Ok(s) => s,
            Err(e) => return Frame::error(format!("ERR invalid schema JSON: {e}")),
        };

        let contract = DataContract::new(name.to_string(), source_id.to_string(), schema);

        let gw = mesh_gateway();
        match gw.add_contract(contract) {
            Ok(()) => Frame::simple("OK"),
            Err(e) => Frame::error(format!("ERR {e}")),
        }
    }

    #[cfg(not(feature = "experimental"))]
    pub(super) async fn handle_federation_contract(
        &self,
        _name: &str,
        _source_id: &str,
        _schema_json: &str,
    ) -> Frame {
        Frame::error("ERR FEDERATION commands require the 'experimental' feature")
    }

    #[cfg(feature = "experimental")]
    pub(super) async fn handle_federation_contracts(&self) -> Frame {
        let gw = mesh_gateway();
        let contracts = gw.list_contracts();
        if contracts.is_empty() {
            return Frame::array(vec![]);
        }
        let items: Vec<Frame> = contracts
            .iter()
            .map(|c| {
                Frame::array(vec![
                    Frame::bulk(c.name.clone()),
                    Frame::bulk(c.source_id.clone()),
                    Frame::bulk(c.status.to_string()),
                    Frame::Integer(c.version as i64),
                ])
            })
            .collect();
        Frame::array(items)
    }

    #[cfg(not(feature = "experimental"))]
    pub(super) async fn handle_federation_contracts(&self) -> Frame {
        Frame::error("ERR FEDERATION commands require the 'experimental' feature")
    }

    #[cfg(feature = "experimental")]
    pub(super) async fn handle_federation_stats(&self) -> Frame {
        use std::sync::atomic::Ordering;

        let gw = mesh_gateway();
        let s = gw.stats();
        Frame::array(vec![
            Frame::bulk("sources_active"),
            Frame::Integer(s.sources_active as i64),
            Frame::bulk("queries_routed"),
            Frame::Integer(s.queries_routed.load(Ordering::Relaxed) as i64),
            Frame::bulk("errors"),
            Frame::Integer(s.errors.load(Ordering::Relaxed) as i64),
            Frame::bulk("namespaces_registered"),
            Frame::Integer(s.namespaces_registered as i64),
            Frame::bulk("contracts_active"),
            Frame::Integer(s.contracts_active as i64),
        ])
    }

    #[cfg(not(feature = "experimental"))]
    pub(super) async fn handle_federation_stats(&self) -> Frame {
        Frame::error("ERR FEDERATION commands require the 'experimental' feature")
    }
}
