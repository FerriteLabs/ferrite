//! Mnemo + Forge: a Forge module would call this bridge's recall
//! shim through a host function.  This test simulates that path:
//! seed memories, then the bridge returns the most-recent K records
//! (the production wiring would replace this with the embedding-aware
//! ranker exposed by `recall_with_embedding`).
//!
//! The `score_record_for_forge` / `parse_forge_result` helpers define a
//! compact wire format for passing records into a WASM scoring function
//! and extracting the resulting score.

use ferrite_mnemo::{MemoryKind, MemoryRecordBuilder, Scope};
use ferrite_spike_bridge::{parse_forge_result, score_record_for_forge, MnemoForgeBridge};

#[test]
fn forge_module_can_recall_mnemo_memories() {
    let bridge = MnemoForgeBridge::new();
    let scope = Scope::new("acme", "agent-7").with_session("s1");

    for (i, txt) in ["first", "second", "third"].iter().enumerate() {
        let rec = MemoryRecordBuilder::new()
            .id(format!("r{i}"))
            .tenant("acme")
            .agent("agent-7")
            .session("s1")
            .kind(MemoryKind::Episodic)
            .content((*txt).to_string())
            .importance(0.5)
            .created_at(1_000 + i as u64)
            .build()
            .expect("valid record");
        bridge.remember(&scope, rec).expect("put");
    }

    let hits = bridge.recall_text(&scope, "anything", 2, 2_000);
    assert_eq!(hits.len(), 2);
    // All hits come from the same agent/tenant scope.
    assert!(hits
        .iter()
        .all(|c| ["first", "second", "third"].contains(&c.as_str())));

    // A different agent sees nothing.
    let other = Scope::new("acme", "agent-other");
    assert!(bridge.recall_text(&other, "anything", 5, 2_000).is_empty());
}

#[test]
fn score_record_round_trips_through_forge_wire_format() {
    let record = MemoryRecordBuilder::new()
        .id("rec-1")
        .tenant("acme")
        .agent("bot-1")
        .kind(MemoryKind::Semantic)
        .content("The quick brown fox".to_string())
        .importance(0.75)
        .created_at(5_000)
        .build()
        .expect("valid record");

    let payload = score_record_for_forge(&record);

    // Manually decode to verify the wire format is correct.
    assert!(payload.len() >= 4 + 8 + 8 + 4);

    let importance = f32::from_le_bytes(payload[0..4].try_into().unwrap());
    let created_at = u64::from_le_bytes(payload[4..12].try_into().unwrap());
    let access_count = u64::from_le_bytes(payload[12..20].try_into().unwrap());
    let content_len = u32::from_le_bytes(payload[20..24].try_into().unwrap()) as usize;
    let content = std::str::from_utf8(&payload[24..24 + content_len]).unwrap();

    assert!((importance - 0.75).abs() < f32::EPSILON);
    assert_eq!(created_at, 5_000);
    assert_eq!(access_count, 0);
    assert_eq!(content, "The quick brown fox");
}

#[test]
fn parse_forge_result_extracts_score() {
    // Simulate a Forge function returning a score of 0.42.
    let score: f32 = 0.42;
    let output = score.to_le_bytes();
    let parsed = parse_forge_result(&output).expect("valid f32");
    assert!((parsed - 0.42).abs() < f32::EPSILON);

    // Malformed outputs return None.
    assert!(parse_forge_result(&[]).is_none());
    assert!(parse_forge_result(&[1, 2]).is_none());
    assert!(parse_forge_result(&[1, 2, 3, 4, 5]).is_none());
}

#[test]
fn end_to_end_mnemo_to_forge_scoring_pipeline() {
    let bridge = MnemoForgeBridge::new();
    let scope = Scope::new("corp", "summarizer");

    // Seed two records with different importance values.
    for (id, text, imp) in [
        ("m1", "meeting notes", 0.9_f32),
        ("m2", "random thought", 0.2),
    ] {
        let rec = MemoryRecordBuilder::new()
            .id(id)
            .tenant("corp")
            .agent("summarizer")
            .kind(MemoryKind::Working)
            .content(text.to_string())
            .importance(imp)
            .created_at(1_000)
            .build()
            .expect("valid");
        bridge.remember(&scope, rec).expect("put");
    }

    // Recall all records, serialize each for Forge, simulate scoring,
    // then parse the results back.
    let filter = ferrite_mnemo::RecallFilter {
        limit: 10,
        ..Default::default()
    };
    let recalled = bridge.store().recall(&scope, 2_000, &filter);
    assert_eq!(recalled.records.len(), 2);

    let mut scores = Vec::new();
    for rec in &recalled.records {
        let payload = score_record_for_forge(rec);
        // Simulate a Forge function that echoes back the importance field.
        let importance_bytes = &payload[0..4];
        let score = parse_forge_result(importance_bytes).expect("parse");
        scores.push((rec.id.clone(), score));
    }

    assert_eq!(scores.len(), 2);
    // Both scores should match the original importance values.
    for (id, score) in &scores {
        match id.as_str() {
            "m1" => assert!((score - 0.9).abs() < f32::EPSILON),
            "m2" => assert!((score - 0.2).abs() < f32::EPSILON),
            other => panic!("unexpected record id: {other}"),
        }
    }
}
