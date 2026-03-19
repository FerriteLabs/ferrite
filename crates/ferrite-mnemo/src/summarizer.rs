//! Memory summarization — groups records by session/time-window and produces Summary records.

use crate::schema::{MemoryKind, MemoryRecord, MemoryRecordBuilder};
use std::collections::HashMap;

/// Strategy for selecting which records to summarize.
#[derive(Debug, Clone)]
pub enum SummarizeStrategy {
    /// Group by session, summarize when count exceeds threshold.
    SessionThreshold { max_records: usize },
    /// Group by time window.
    TimeWindow { window_ms: u64 },
}

/// Result of a summarization pass.
#[derive(Debug, Clone)]
pub struct SummarizeResult {
    pub summaries_created: usize,
    pub records_consumed: usize,
    pub records_retained: usize,
}

/// Identify candidate records for summarization.
///
/// Returns groups of records eligible for summarization.  Only non-Summary
/// records are considered — existing summaries are never re-summarized.
pub fn find_candidates<'a>(
    records: &'a [MemoryRecord],
    strategy: &SummarizeStrategy,
) -> Vec<Vec<&'a MemoryRecord>> {
    // Exclude existing summaries from candidate selection.
    let eligible: Vec<&MemoryRecord> = records
        .iter()
        .filter(|r| r.kind != MemoryKind::Summary)
        .collect();

    match strategy {
        SummarizeStrategy::SessionThreshold { max_records } => {
            // Group by session_id, return groups exceeding threshold.
            let mut by_session: HashMap<Option<&str>, Vec<&'a MemoryRecord>> = HashMap::new();
            for r in &eligible {
                by_session
                    .entry(r.session_id.as_deref())
                    .or_default()
                    .push(r);
            }
            by_session
                .into_values()
                .filter(|group| group.len() >= *max_records)
                .collect()
        }
        SummarizeStrategy::TimeWindow { window_ms } => {
            if *window_ms == 0 {
                return Vec::new();
            }
            // Group by time windows, return complete windows.
            let mut by_window: HashMap<u64, Vec<&'a MemoryRecord>> = HashMap::new();
            for r in &eligible {
                let bucket = r.created_at / window_ms;
                by_window.entry(bucket).or_default().push(r);
            }
            // Return all non-empty windows (sorted by bucket for determinism).
            let mut buckets: Vec<u64> = by_window.keys().copied().collect();
            buckets.sort_unstable();
            buckets
                .into_iter()
                .filter_map(|b| {
                    let group = by_window.remove(&b)?;
                    if group.is_empty() {
                        None
                    } else {
                        Some(group)
                    }
                })
                .collect()
        }
    }
}

/// Create a summary record from a batch of records.
///
/// In production, this would call an LLM.  For now, concatenate content with
/// newline separators and tag the record as `MemoryKind::Summary`.
pub fn create_summary(
    batch: &[&MemoryRecord],
    agent_id: &str,
    session_id: Option<&str>,
    now_ms: u64,
) -> MemoryRecord {
    let content: String = batch
        .iter()
        .map(|r| r.content.as_str())
        .collect::<Vec<_>>()
        .join("\n");

    let tenant_id = batch
        .first()
        .map(|r| r.tenant_id.as_str())
        .unwrap_or("default");

    let id = format!("summary-{}", now_ms);

    let mut builder = MemoryRecordBuilder::new()
        .id(&id)
        .tenant(tenant_id)
        .agent(agent_id)
        .kind(MemoryKind::Summary)
        .content(content)
        .importance(0.7)
        .created_at(now_ms);

    if let Some(sid) = session_id {
        builder = builder.session(sid);
    }

    // Propagate source record IDs in metadata.
    let source_ids: Vec<String> = batch.iter().map(|r| r.id.clone()).collect();
    builder = builder.metadata("source_ids", serde_json::json!(source_ids));

    builder
        .build()
        .expect("summary record build should not fail")
}

/// Run a full summarization pass on the given records.
///
/// Returns the `SummarizeResult` and a list of newly created summary records.
pub fn summarize(
    records: &[MemoryRecord],
    agent_id: &str,
    strategy: &SummarizeStrategy,
    now_ms: u64,
) -> (SummarizeResult, Vec<MemoryRecord>) {
    let groups = find_candidates(records, strategy);
    let mut summaries = Vec::new();
    let mut consumed = 0usize;

    for group in &groups {
        let session_id = group.first().and_then(|r| r.session_id.as_deref());
        let summary = create_summary(group, agent_id, session_id, now_ms + summaries.len() as u64);
        consumed += group.len();
        summaries.push(summary);
    }

    let result = SummarizeResult {
        summaries_created: summaries.len(),
        records_consumed: consumed,
        records_retained: records.len() - consumed,
    };

    (result, summaries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{MemoryKind, MemoryRecordBuilder};

    fn make_rec(id: &str, session: Option<&str>, ts: u64) -> MemoryRecord {
        let mut builder = MemoryRecordBuilder::new()
            .id(id)
            .tenant("t")
            .agent("a")
            .kind(MemoryKind::Episodic)
            .content(format!("content-{id}"))
            .importance(0.5)
            .created_at(ts);
        if let Some(s) = session {
            builder = builder.session(s);
        }
        builder.build().unwrap()
    }

    fn make_summary(id: &str, ts: u64) -> MemoryRecord {
        MemoryRecordBuilder::new()
            .id(id)
            .tenant("t")
            .agent("a")
            .kind(MemoryKind::Summary)
            .content("existing summary")
            .importance(0.7)
            .created_at(ts)
            .build()
            .unwrap()
    }

    // -----------------------------------------------------------------------
    // find_candidates — SessionThreshold
    // -----------------------------------------------------------------------

    #[test]
    fn session_threshold_groups_correctly() {
        let records = vec![
            make_rec("r1", Some("s1"), 100),
            make_rec("r2", Some("s1"), 200),
            make_rec("r3", Some("s1"), 300),
            make_rec("r4", Some("s2"), 100),
        ];
        let groups = find_candidates(
            &records,
            &SummarizeStrategy::SessionThreshold { max_records: 3 },
        );
        // Only session s1 has ≥ 3 records
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].len(), 3);
        assert!(groups[0]
            .iter()
            .all(|r| r.session_id.as_deref() == Some("s1")));
    }

    #[test]
    fn session_threshold_below_threshold_returns_empty() {
        let records = vec![
            make_rec("r1", Some("s1"), 100),
            make_rec("r2", Some("s2"), 200),
        ];
        let groups = find_candidates(
            &records,
            &SummarizeStrategy::SessionThreshold { max_records: 5 },
        );
        assert!(groups.is_empty());
    }

    #[test]
    fn session_threshold_excludes_existing_summaries() {
        let records = vec![
            make_rec("r1", Some("s1"), 100),
            make_rec("r2", Some("s1"), 200),
            make_summary("sum1", 300),
        ];
        let groups = find_candidates(
            &records,
            &SummarizeStrategy::SessionThreshold { max_records: 2 },
        );
        assert_eq!(groups.len(), 1);
        assert!(groups[0].iter().all(|r| r.kind != MemoryKind::Summary));
    }

    // -----------------------------------------------------------------------
    // find_candidates — TimeWindow
    // -----------------------------------------------------------------------

    #[test]
    fn time_window_groups_correctly() {
        let records = vec![
            make_rec("r1", None, 0),
            make_rec("r2", None, 50),
            make_rec("r3", None, 100),
            make_rec("r4", None, 150),
        ];
        let groups = find_candidates(&records, &SummarizeStrategy::TimeWindow { window_ms: 100 });
        // bucket 0: [0, 50], bucket 1: [100, 150]
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].len(), 2);
        assert_eq!(groups[1].len(), 2);
    }

    #[test]
    fn time_window_zero_returns_empty() {
        let records = vec![make_rec("r1", None, 100)];
        let groups = find_candidates(&records, &SummarizeStrategy::TimeWindow { window_ms: 0 });
        assert!(groups.is_empty());
    }

    // -----------------------------------------------------------------------
    // create_summary
    // -----------------------------------------------------------------------

    #[test]
    fn create_summary_produces_summary_kind() {
        let r1 = make_rec("r1", Some("s1"), 100);
        let r2 = make_rec("r2", Some("s1"), 200);
        let batch: Vec<&MemoryRecord> = vec![&r1, &r2];
        let summary = create_summary(&batch, "a", Some("s1"), 1000);

        assert_eq!(summary.kind, MemoryKind::Summary);
        assert_eq!(summary.agent_id, "a");
        assert_eq!(summary.session_id.as_deref(), Some("s1"));
        assert!(summary.content.contains("content-r1"));
        assert!(summary.content.contains("content-r2"));
        assert_eq!(summary.created_at, 1000);

        // Check source_ids metadata
        let source_ids = summary.metadata.get("source_ids").unwrap();
        let ids: Vec<String> = serde_json::from_value(source_ids.clone()).unwrap();
        assert_eq!(ids, vec!["r1", "r2"]);
    }

    #[test]
    fn create_summary_without_session() {
        let r1 = make_rec("r1", None, 100);
        let batch: Vec<&MemoryRecord> = vec![&r1];
        let summary = create_summary(&batch, "a", None, 500);
        assert_eq!(summary.kind, MemoryKind::Summary);
        assert!(summary.session_id.is_none());
    }

    // -----------------------------------------------------------------------
    // summarize (full pass)
    // -----------------------------------------------------------------------

    #[test]
    fn summarize_full_pass() {
        let records = vec![
            make_rec("r1", Some("s1"), 100),
            make_rec("r2", Some("s1"), 200),
            make_rec("r3", Some("s1"), 300),
            make_rec("r4", Some("s2"), 400),
        ];
        let (result, summaries) = summarize(
            &records,
            "a",
            &SummarizeStrategy::SessionThreshold { max_records: 3 },
            1000,
        );
        assert_eq!(result.summaries_created, 1);
        assert_eq!(result.records_consumed, 3);
        assert_eq!(result.records_retained, 1);
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].kind, MemoryKind::Summary);
    }
}
