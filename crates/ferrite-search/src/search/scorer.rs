//! Scoring algorithms for full-text search
//!
//! Implements BM25 and TF-IDF ranking algorithms.

use std::collections::HashMap;

/// Scoring parameters
#[derive(Debug, Clone)]
pub struct ScoringParams {
    /// BM25 k1 parameter (term frequency saturation)
    pub k1: f32,
    /// BM25 b parameter (length normalization)
    pub b: f32,
    /// Field weights
    pub field_weights: HashMap<String, f32>,
    /// Minimum score threshold
    pub min_score: Option<f32>,
    /// Enable coordination factor
    pub coord: bool,
}

impl Default for ScoringParams {
    fn default() -> Self {
        Self {
            k1: 1.2,
            b: 0.75,
            field_weights: HashMap::new(),
            min_score: None,
            coord: true,
        }
    }
}

impl ScoringParams {
    /// Create with custom k1 and b
    pub fn new(k1: f32, b: f32) -> Self {
        Self {
            k1,
            b,
            ..Default::default()
        }
    }

    /// Set field weight
    pub fn field_weight(mut self, field: impl Into<String>, weight: f32) -> Self {
        self.field_weights.insert(field.into(), weight);
        self
    }

    /// Set minimum score
    pub fn min_score(mut self, score: f32) -> Self {
        self.min_score = Some(score);
        self
    }

    /// Enable/disable coordination factor
    pub fn coord(mut self, enabled: bool) -> Self {
        self.coord = enabled;
        self
    }
}

/// Scorer trait for ranking documents
pub trait Scorer: Send + Sync {
    /// Score a document for a term
    fn score(
        &self,
        term_freq: u32,
        doc_freq: u32,
        doc_length: u32,
        avg_doc_length: f32,
        total_docs: u32,
    ) -> f32;

    /// Compute IDF component
    fn idf(&self, doc_freq: u32, total_docs: u32) -> f32;

    /// Combine scores for multiple terms
    fn combine(&self, scores: &[f32]) -> f32 {
        scores.iter().sum()
    }

    /// Get scorer name
    fn name(&self) -> &str;
}

/// BM25 scorer (Okapi BM25)
#[derive(Debug, Clone)]
pub struct BM25Scorer {
    /// Parameters
    params: ScoringParams,
}

impl BM25Scorer {
    /// Create a new BM25 scorer
    pub fn new() -> Self {
        Self {
            params: ScoringParams::default(),
        }
    }

    /// Create with custom parameters
    pub fn with_params(params: ScoringParams) -> Self {
        Self { params }
    }

    /// Get k1 parameter
    pub fn k1(&self) -> f32 {
        self.params.k1
    }

    /// Get b parameter
    pub fn b(&self) -> f32 {
        self.params.b
    }
}

impl Default for BM25Scorer {
    fn default() -> Self {
        Self::new()
    }
}

impl Scorer for BM25Scorer {
    fn score(
        &self,
        term_freq: u32,
        doc_freq: u32,
        doc_length: u32,
        avg_doc_length: f32,
        total_docs: u32,
    ) -> f32 {
        let tf = term_freq as f32;
        let dl = doc_length as f32;
        let avgdl = avg_doc_length;
        let k1 = self.params.k1;
        let b = self.params.b;

        // IDF component: log((N - n + 0.5) / (n + 0.5) + 1)
        let idf = self.idf(doc_freq, total_docs);

        // TF component: (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * dl/avgdl))
        let tf_component = (tf * (k1 + 1.0)) / (tf + k1 * (1.0 - b + b * dl / avgdl));

        idf * tf_component
    }

    fn idf(&self, doc_freq: u32, total_docs: u32) -> f32 {
        let n = total_docs as f32;
        let df = doc_freq as f32;
        ((n - df + 0.5) / (df + 0.5) + 1.0).ln()
    }

    fn name(&self) -> &str {
        "BM25"
    }
}

/// TF-IDF scorer
#[derive(Debug, Clone)]
pub struct TfIdfScorer {
    /// Use sublinear TF scaling (1 + log(tf))
    sublinear_tf: bool,
    /// Use smooth IDF
    smooth_idf: bool,
}

impl TfIdfScorer {
    /// Create a new TF-IDF scorer
    pub fn new() -> Self {
        Self {
            sublinear_tf: true,
            smooth_idf: true,
        }
    }

    /// Enable/disable sublinear TF scaling
    pub fn sublinear_tf(mut self, enabled: bool) -> Self {
        self.sublinear_tf = enabled;
        self
    }

    /// Enable/disable smooth IDF
    pub fn smooth_idf(mut self, enabled: bool) -> Self {
        self.smooth_idf = enabled;
        self
    }
}

impl Default for TfIdfScorer {
    fn default() -> Self {
        Self::new()
    }
}

impl Scorer for TfIdfScorer {
    fn score(
        &self,
        term_freq: u32,
        doc_freq: u32,
        _doc_length: u32,
        _avg_doc_length: f32,
        total_docs: u32,
    ) -> f32 {
        let tf = if self.sublinear_tf {
            if term_freq > 0 {
                1.0 + (term_freq as f32).ln()
            } else {
                0.0
            }
        } else {
            term_freq as f32
        };

        let idf = self.idf(doc_freq, total_docs);

        tf * idf
    }

    fn idf(&self, doc_freq: u32, total_docs: u32) -> f32 {
        let n = total_docs as f32;
        let df = doc_freq as f32;

        if self.smooth_idf {
            // Smooth IDF: log((N + 1) / (df + 1)) + 1
            ((n + 1.0) / (df + 1.0)).ln() + 1.0
        } else {
            // Standard IDF: log(N / df) + 1
            if df > 0.0 {
                (n / df).ln() + 1.0
            } else {
                0.0
            }
        }
    }

    fn name(&self) -> &str {
        "TF-IDF"
    }
}

/// Boolean scorer (no ranking, just matching)
#[derive(Debug, Clone, Default)]
pub struct BooleanScorer;

impl BooleanScorer {
    /// Create a new boolean scorer
    pub fn new() -> Self {
        Self
    }
}

impl Scorer for BooleanScorer {
    fn score(
        &self,
        term_freq: u32,
        _doc_freq: u32,
        _doc_length: u32,
        _avg_doc_length: f32,
        _total_docs: u32,
    ) -> f32 {
        if term_freq > 0 {
            1.0
        } else {
            0.0
        }
    }

    fn idf(&self, _doc_freq: u32, _total_docs: u32) -> f32 {
        1.0
    }

    fn name(&self) -> &str {
        "Boolean"
    }
}

/// Constant scorer
#[derive(Debug, Clone)]
pub struct ConstantScorer {
    score: f32,
}

impl ConstantScorer {
    /// Create with constant score
    pub fn new(score: f32) -> Self {
        Self { score }
    }
}

impl Default for ConstantScorer {
    fn default() -> Self {
        Self::new(1.0)
    }
}

impl Scorer for ConstantScorer {
    fn score(
        &self,
        _term_freq: u32,
        _doc_freq: u32,
        _doc_length: u32,
        _avg_doc_length: f32,
        _total_docs: u32,
    ) -> f32 {
        self.score
    }

    fn idf(&self, _doc_freq: u32, _total_docs: u32) -> f32 {
        1.0
    }

    fn name(&self) -> &str {
        "Constant"
    }
}

/// DFR (Divergence From Randomness) scorer
#[derive(Debug, Clone)]
pub struct DfrScorer {
    /// Basic model
    basic_model: DfrBasicModel,
    /// After effect
    after_effect: DfrAfterEffect,
    /// Normalization
    normalization: DfrNormalization,
}

/// DFR basic model
#[derive(Debug, Clone, Copy)]
pub enum DfrBasicModel {
    /// Geometric distribution
    G,
    /// Poisson distribution
    P,
    /// Bose-Einstein
    Be,
    /// Inverse document frequency
    I,
}

/// DFR after effect
#[derive(Debug, Clone, Copy)]
pub enum DfrAfterEffect {
    /// Laplace law of succession
    L,
    /// Ratio of two Bernoulli processes
    B,
}

/// DFR normalization
#[derive(Debug, Clone, Copy)]
pub enum DfrNormalization {
    /// Normalization H1
    H1,
    /// Normalization H2
    H2,
    /// No normalization
    None,
}

impl DfrScorer {
    /// Create with default parameters
    pub fn new() -> Self {
        Self {
            basic_model: DfrBasicModel::G,
            after_effect: DfrAfterEffect::L,
            normalization: DfrNormalization::H2,
        }
    }

    /// Set basic model
    pub fn basic_model(mut self, model: DfrBasicModel) -> Self {
        self.basic_model = model;
        self
    }

    /// Set after effect
    pub fn after_effect(mut self, ae: DfrAfterEffect) -> Self {
        self.after_effect = ae;
        self
    }

    /// Set normalization
    pub fn normalization(mut self, norm: DfrNormalization) -> Self {
        self.normalization = norm;
        self
    }
}

impl Default for DfrScorer {
    fn default() -> Self {
        Self::new()
    }
}

impl Scorer for DfrScorer {
    fn score(
        &self,
        term_freq: u32,
        doc_freq: u32,
        doc_length: u32,
        avg_doc_length: f32,
        total_docs: u32,
    ) -> f32 {
        let tf = term_freq as f32;
        let df = doc_freq as f32;
        let dl = doc_length as f32;
        let avgdl = avg_doc_length;
        let n = total_docs as f32;

        // Normalized term frequency
        let tfn = match self.normalization {
            DfrNormalization::H1 => tf * avgdl / dl,
            DfrNormalization::H2 => tf * (avgdl / dl).ln(),
            DfrNormalization::None => tf,
        };

        // Basic model score
        let basic = match self.basic_model {
            DfrBasicModel::G => {
                let lambda = df / n;
                // G model: log(1 + lambda) + tfn * log(lambda / (1 + lambda))
                // Avoids log of negative numbers
                (1.0 + lambda).ln() + tfn * (lambda / (1.0 + lambda)).ln()
            }
            DfrBasicModel::P => {
                let lambda = df / n;
                tfn * (tfn / lambda).ln() + (lambda - tfn) * std::f32::consts::E.ln()
            }
            DfrBasicModel::Be => {
                let _f = df / n;
                -((n + 1.0) / (df + 0.5)).ln()
            }
            DfrBasicModel::I => self.idf(doc_freq, total_docs),
        };

        // After effect
        let gain = match self.after_effect {
            DfrAfterEffect::L => 1.0 / (tfn + 1.0),
            DfrAfterEffect::B => (n + 1.0) / (df * tfn + 1.0),
        };

        basic * gain
    }

    fn idf(&self, doc_freq: u32, total_docs: u32) -> f32 {
        let n = total_docs as f32;
        let df = doc_freq as f32;
        ((n + 1.0) / (df + 0.5)).ln()
    }

    fn name(&self) -> &str {
        "DFR"
    }
}

/// Score explanation for debugging
#[derive(Debug, Clone)]
pub struct ScoreExplanation {
    /// Scorer name
    pub scorer: String,
    /// Total score
    pub score: f32,
    /// Component explanations
    pub components: Vec<ComponentExplanation>,
}

/// Component of score explanation
#[derive(Debug, Clone)]
pub struct ComponentExplanation {
    /// Component name
    pub name: String,
    /// Component value
    pub value: f32,
    /// Description
    pub description: String,
}

impl ScoreExplanation {
    /// Create a new explanation
    pub fn new(scorer: &str, score: f32) -> Self {
        Self {
            scorer: scorer.to_string(),
            score,
            components: Vec::new(),
        }
    }

    /// Add a component
    pub fn add_component(&mut self, name: &str, value: f32, description: &str) {
        self.components.push(ComponentExplanation {
            name: name.to_string(),
            value,
            description: description.to_string(),
        });
    }

    /// Format as string
    pub fn to_string_pretty(&self) -> String {
        let mut s = format!("{} score: {:.4}\n", self.scorer, self.score);
        for comp in &self.components {
            s.push_str(&format!(
                "  {}: {:.4} ({})\n",
                comp.name, comp.value, comp.description
            ));
        }
        s
    }
}

/// Explainable scorer wrapper
pub struct ExplainableScorer<S: Scorer> {
    inner: S,
}

impl<S: Scorer> ExplainableScorer<S> {
    /// Create a new explainable scorer
    pub fn new(scorer: S) -> Self {
        Self { inner: scorer }
    }

    /// Score with explanation
    pub fn score_explain(
        &self,
        term_freq: u32,
        doc_freq: u32,
        doc_length: u32,
        avg_doc_length: f32,
        total_docs: u32,
    ) -> ScoreExplanation {
        let score = self
            .inner
            .score(term_freq, doc_freq, doc_length, avg_doc_length, total_docs);
        let idf = self.inner.idf(doc_freq, total_docs);

        let mut explanation = ScoreExplanation::new(self.inner.name(), score);
        explanation.add_component(
            "tf",
            term_freq as f32,
            "term frequency in document",
        );
        explanation.add_component("df", doc_freq as f32, "documents containing term");
        explanation.add_component("idf", idf, "inverse document frequency");
        explanation.add_component("dl", doc_length as f32, "document length");
        explanation.add_component("avgdl", avg_doc_length, "average document length");
        explanation.add_component("N", total_docs as f32, "total documents");

        explanation
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bm25_scorer() {
        let scorer = BM25Scorer::new();

        // Basic scoring
        let score = scorer.score(5, 100, 200, 250.0, 10000);
        assert!(score > 0.0);

        // Higher TF should give higher score
        let score_high_tf = scorer.score(10, 100, 200, 250.0, 10000);
        assert!(score_high_tf > score);

        // Rarer terms (lower DF) should have higher IDF
        let idf_rare = scorer.idf(10, 10000);
        let idf_common = scorer.idf(1000, 10000);
        assert!(idf_rare > idf_common);
    }

    #[test]
    fn test_bm25_parameters() {
        let scorer = BM25Scorer::with_params(ScoringParams::new(2.0, 0.5));
        assert_eq!(scorer.k1(), 2.0);
        assert_eq!(scorer.b(), 0.5);
    }

    #[test]
    fn test_tfidf_scorer() {
        let scorer = TfIdfScorer::new();

        let score = scorer.score(5, 100, 200, 250.0, 10000);
        assert!(score > 0.0);

        // Zero TF should give zero score
        let zero_score = scorer.score(0, 100, 200, 250.0, 10000);
        assert_eq!(zero_score, 0.0);
    }

    #[test]
    fn test_tfidf_sublinear() {
        let sublinear = TfIdfScorer::new().sublinear_tf(true);
        let linear = TfIdfScorer::new().sublinear_tf(false);

        let score_sub = sublinear.score(100, 50, 200, 250.0, 10000);
        let score_lin = linear.score(100, 50, 200, 250.0, 10000);

        // Linear should scale faster
        assert!(score_lin > score_sub);
    }

    #[test]
    fn test_boolean_scorer() {
        let scorer = BooleanScorer::new();

        assert_eq!(scorer.score(1, 100, 200, 250.0, 10000), 1.0);
        assert_eq!(scorer.score(5, 100, 200, 250.0, 10000), 1.0);
        assert_eq!(scorer.score(0, 100, 200, 250.0, 10000), 0.0);
    }

    #[test]
    fn test_constant_scorer() {
        let scorer = ConstantScorer::new(5.0);

        assert_eq!(scorer.score(1, 100, 200, 250.0, 10000), 5.0);
        assert_eq!(scorer.score(100, 10, 50, 100.0, 5000), 5.0);
    }

    #[test]
    fn test_dfr_scorer() {
        let scorer = DfrScorer::new();

        let score = scorer.score(5, 100, 200, 250.0, 10000);
        // DFR can produce various scores depending on model
        assert!(!score.is_nan());
    }

    #[test]
    fn test_scoring_params() {
        let params = ScoringParams::default()
            .field_weight("title", 2.0)
            .field_weight("body", 1.0)
            .min_score(0.5)
            .coord(false);

        assert_eq!(params.field_weights.get("title"), Some(&2.0));
        assert_eq!(params.min_score, Some(0.5));
        assert!(!params.coord);
    }

    #[test]
    fn test_score_combine() {
        let scorer = BM25Scorer::new();

        let scores = vec![1.0, 2.0, 3.0];
        let combined = scorer.combine(&scores);
        assert_eq!(combined, 6.0);
    }

    #[test]
    fn test_explainable_scorer() {
        let scorer = ExplainableScorer::new(BM25Scorer::new());

        let explanation = scorer.score_explain(5, 100, 200, 250.0, 10000);

        assert_eq!(explanation.scorer, "BM25");
        assert!(explanation.score > 0.0);
        assert!(!explanation.components.is_empty());
    }

    #[test]
    fn test_explanation_format() {
        let mut explanation = ScoreExplanation::new("BM25", 3.14);
        explanation.add_component("tf", 5.0, "term frequency");
        explanation.add_component("idf", 2.5, "inverse document frequency");

        let formatted = explanation.to_string_pretty();
        assert!(formatted.contains("BM25"));
        assert!(formatted.contains("3.14"));
        assert!(formatted.contains("tf"));
    }

    #[test]
    fn test_bm25_edge_cases() {
        let scorer = BM25Scorer::new();

        // Very short document
        let score_short = scorer.score(5, 100, 10, 250.0, 10000);

        // Very long document
        let score_long = scorer.score(5, 100, 1000, 250.0, 10000);

        // Short documents should score higher for same TF
        assert!(score_short > score_long);
    }

    #[test]
    fn test_idf_bounds() {
        let bm25 = BM25Scorer::new();
        let tfidf = TfIdfScorer::new();

        // Very rare term
        let idf_rare_bm25 = bm25.idf(1, 1000000);
        let idf_rare_tfidf = tfidf.idf(1, 1000000);

        // Very common term
        let idf_common_bm25 = bm25.idf(999999, 1000000);
        let idf_common_tfidf = tfidf.idf(999999, 1000000);

        // IDF should always be positive
        assert!(idf_rare_bm25 > 0.0);
        assert!(idf_rare_tfidf > 0.0);
        assert!(idf_common_bm25 > 0.0);
        assert!(idf_common_tfidf > 0.0);
    }
}
