//! Multi-language support for full-text search
//!
//! Provides language detection, per-language stopword lists,
//! and per-language stemmer selection.

use super::analyzer::{
    Analyzer, LowercaseFilter, PorterStemmer, StandardTokenizer, StopWordFilter, Token,
    TokenFilter, Tokenizer,
};
use std::collections::HashMap;

/// Supported languages
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Language {
    English,
    Spanish,
    French,
    German,
    Portuguese,
    Italian,
}

impl Language {
    /// Get the language name
    pub fn name(&self) -> &str {
        match self {
            Language::English => "english",
            Language::Spanish => "spanish",
            Language::French => "french",
            Language::German => "german",
            Language::Portuguese => "portuguese",
            Language::Italian => "italian",
        }
    }

    /// Get stopwords for this language
    pub fn stopwords(&self) -> &[&str] {
        match self {
            Language::English => &ENGLISH_STOPWORDS,
            Language::Spanish => &SPANISH_STOPWORDS,
            Language::French => &FRENCH_STOPWORDS,
            Language::German => &GERMAN_STOPWORDS,
            Language::Portuguese => &PORTUGUESE_STOPWORDS,
            Language::Italian => &ITALIAN_STOPWORDS,
        }
    }

    /// Build a StopWordFilter for this language
    pub fn stopword_filter(&self) -> StopWordFilter {
        StopWordFilter::new(self.stopwords())
    }

    /// Build a stemmer filter for this language.
    /// Currently uses Porter stemmer for English; other languages
    /// use a language-specific suffix-stripping stemmer.
    pub fn stemmer_filter(&self) -> Box<dyn TokenFilter> {
        match self {
            Language::English => Box::new(PorterStemmer::new()),
            Language::Spanish => Box::new(SuffixStemmer::spanish()),
            Language::French => Box::new(SuffixStemmer::french()),
            Language::German => Box::new(SuffixStemmer::german()),
            Language::Portuguese => Box::new(SuffixStemmer::portuguese()),
            Language::Italian => Box::new(SuffixStemmer::italian()),
        }
    }
}

impl std::fmt::Display for Language {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

/// Simple suffix-stripping stemmer for non-English languages
pub struct SuffixStemmer {
    suffixes: Vec<(&'static str, &'static str)>,
}

impl SuffixStemmer {
    fn new(suffixes: Vec<(&'static str, &'static str)>) -> Self {
        Self { suffixes }
    }

    fn spanish() -> Self {
        Self::new(vec![
            ("aciones", ""),
            ("mente", ""),
            ("idad", ""),
            ("ando", ""),
            ("iendo", ""),
            ("ado", ""),
            ("ido", ""),
            ("es", ""),
            ("s", ""),
        ])
    }

    fn french() -> Self {
        Self::new(vec![
            ("issement", ""),
            ("ement", ""),
            ("ment", ""),
            ("tion", ""),
            ("eux", ""),
            ("ant", ""),
            ("es", ""),
            ("s", ""),
        ])
    }

    fn german() -> Self {
        Self::new(vec![
            ("ungen", ""),
            ("heit", ""),
            ("keit", ""),
            ("isch", ""),
            ("ung", ""),
            ("ig", ""),
            ("en", ""),
            ("er", ""),
            ("es", ""),
            ("e", ""),
        ])
    }

    fn portuguese() -> Self {
        Self::new(vec![
            ("mente", ""),
            ("ções", ""),
            ("ção", ""),
            ("ando", ""),
            ("endo", ""),
            ("ado", ""),
            ("ido", ""),
            ("es", ""),
            ("s", ""),
        ])
    }

    fn italian() -> Self {
        Self::new(vec![
            ("mente", ""),
            ("zione", ""),
            ("ando", ""),
            ("endo", ""),
            ("ato", ""),
            ("ito", ""),
            ("are", ""),
            ("ere", ""),
            ("ire", ""),
            ("i", ""),
            ("e", ""),
        ])
    }

    fn stem(&self, word: &str) -> String {
        let mut result = word.to_lowercase();
        for (suffix, replacement) in &self.suffixes {
            if result.ends_with(suffix) && result.len() > suffix.len() + 2 {
                result = format!("{}{}", &result[..result.len() - suffix.len()], replacement);
                break;
            }
        }
        result
    }
}

impl TokenFilter for SuffixStemmer {
    fn filter(&self, tokens: Vec<Token>) -> Vec<Token> {
        tokens
            .into_iter()
            .map(|mut t| {
                t.text = self.stem(&t.text);
                t
            })
            .collect()
    }
}

/// Language detector using character n-gram frequency heuristics
pub struct LanguageDetector {
    default_language: Language,
    profiles: HashMap<Language, HashMap<String, f32>>,
}

impl LanguageDetector {
    /// Create a new detector with a default language
    pub fn new(default_language: Language) -> Self {
        let mut detector = Self {
            default_language,
            profiles: HashMap::new(),
        };
        detector.load_profiles();
        detector
    }

    /// Detect the language of the given text.
    /// Falls back to the default language if detection is uncertain.
    pub fn detect(&self, text: &str) -> Language {
        if text.len() < 10 {
            return self.default_language;
        }

        let text_profile = self.build_profile(text);

        let mut best_lang = self.default_language;
        let mut best_score = f32::MAX;

        for (lang, profile) in &self.profiles {
            let score = self.distance(&text_profile, profile);
            if score < best_score {
                best_score = score;
                best_lang = *lang;
            }
        }

        best_lang
    }

    fn load_profiles(&mut self) {
        // Use characteristic word fragments as language signatures
        let profiles: &[(Language, &[&str])] = &[
            (
                Language::English,
                &[
                    "the", "th", "he", "in", "an", "nd", "er", "on", "is", "of", "it", "ng", "at",
                    "en", "ed", "al", "ou", "re",
                ],
            ),
            (
                Language::Spanish,
                &[
                    "el", "la", "qu", "ue", "es", "en", "de", "rq", "po", "al", "lo", "ci", "ar",
                    "er", "un", "as", "os", "or",
                ],
            ),
            (
                Language::French,
                &[
                    "le", "de", "es", "en", "la", "ou", "qu", "nt", "ai", "on", "re", "ur", "an",
                    "eu", "er", "et", "un", "io",
                ],
            ),
            (
                Language::German,
                &[
                    "de", "er", "en", "ch", "ei", "ie", "in", "nd", "ge", "di", "te", "un", "be",
                    "sc", "au", "st", "an", "da",
                ],
            ),
            (
                Language::Portuguese,
                &[
                    "do", "da", "os", "as", "ão", "de", "em", "um", "qu", "es", "er", "al", "ar",
                    "ou", "nh", "lh", "çã", "na",
                ],
            ),
            (
                Language::Italian,
                &[
                    "di", "il", "la", "in", "ch", "re", "er", "el", "on", "to", "al", "ti", "an",
                    "at", "co", "no", "le", "io",
                ],
            ),
        ];

        for (lang, trigrams) in profiles {
            let mut profile = HashMap::new();
            let total = trigrams.len() as f32;
            for (i, gram) in trigrams.iter().enumerate() {
                profile.insert(gram.to_string(), (total - i as f32) / total);
            }
            self.profiles.insert(*lang, profile);
        }
    }

    fn build_profile(&self, text: &str) -> HashMap<String, f32> {
        let text_lower = text.to_lowercase();
        let chars: Vec<char> = text_lower.chars().collect();
        let mut counts: HashMap<String, u32> = HashMap::new();
        let mut total = 0u32;

        for window in chars.windows(2) {
            let bigram: String = window.iter().collect();
            if bigram.chars().all(|c| c.is_alphabetic()) {
                *counts.entry(bigram).or_insert(0) += 1;
                total += 1;
            }
        }

        let total = total.max(1) as f32;
        counts
            .into_iter()
            .map(|(k, v)| (k, v as f32 / total))
            .collect()
    }

    fn distance(&self, a: &HashMap<String, f32>, b: &HashMap<String, f32>) -> f32 {
        let mut dist = 0.0f32;
        for (key, a_val) in a {
            let b_val = b.get(key).copied().unwrap_or(0.0);
            dist += (a_val - b_val).powi(2);
        }
        for (key, b_val) in b {
            if !a.contains_key(key) {
                dist += b_val.powi(2);
            }
        }
        dist
    }
}

impl Default for LanguageDetector {
    fn default() -> Self {
        Self::new(Language::English)
    }
}

/// Language-aware analyzer that selects stopwords and stemmer
/// based on the configured language.
pub struct LanguageAnalyzer {
    language: Language,
    tokenizer: StandardTokenizer,
    filters: Vec<Box<dyn TokenFilter>>,
}

impl LanguageAnalyzer {
    /// Create an analyzer for the given language
    pub fn new(language: Language) -> Self {
        let filters: Vec<Box<dyn TokenFilter>> = vec![
            Box::new(LowercaseFilter),
            Box::new(language.stopword_filter()),
            language.stemmer_filter(),
        ];

        Self {
            language,
            tokenizer: StandardTokenizer::new(),
            filters,
        }
    }

    /// Get the configured language
    pub fn language(&self) -> Language {
        self.language
    }
}

impl Analyzer for LanguageAnalyzer {
    fn analyze(&self, text: &str) -> Vec<Token> {
        let mut tokens = self.tokenizer.tokenize(text);
        for filter in &self.filters {
            tokens = filter.filter(tokens);
        }
        tokens
    }

    fn tokenizer(&self) -> &dyn Tokenizer {
        &self.tokenizer
    }

    fn token_filters(&self) -> &[Box<dyn TokenFilter>] {
        &self.filters
    }
}

// --- Stopword lists ---

const ENGLISH_STOPWORDS: [&str; 50] = [
    "a", "an", "and", "are", "as", "at", "be", "by", "for", "from", "has", "he", "in", "is", "it",
    "its", "of", "on", "that", "the", "to", "was", "were", "will", "with", "this", "but", "they",
    "have", "had", "what", "when", "where", "who", "which", "why", "how", "all", "each", "every",
    "both", "few", "more", "most", "other", "some", "such", "no", "not", "only",
];

const SPANISH_STOPWORDS: [&str; 40] = [
    "de", "la", "que", "el", "en", "y", "a", "los", "del", "se", "las", "por", "un", "para", "con",
    "no", "una", "su", "al", "lo", "como", "más", "pero", "sus", "le", "ya", "o", "este", "si",
    "porque", "esta", "entre", "cuando", "muy", "sin", "sobre", "también", "me", "hasta", "hay",
];

const FRENCH_STOPWORDS: [&str; 40] = [
    "le", "de", "un", "être", "et", "à", "il", "avoir", "ne", "je", "son", "que", "se", "qui",
    "ce", "dans", "en", "du", "elle", "au", "pas", "pour", "que", "une", "par", "sur", "avec",
    "tout", "nous", "sa", "mais", "ou", "si", "leur", "plus", "ces", "aussi", "comme", "on", "les",
];

const GERMAN_STOPWORDS: [&str; 40] = [
    "der", "die", "und", "in", "den", "von", "zu", "das", "mit", "sich", "des", "auf", "für",
    "ist", "im", "dem", "nicht", "ein", "eine", "als", "auch", "es", "an", "werden", "aus", "er",
    "hat", "dass", "sie", "nach", "wird", "bei", "einer", "um", "am", "sind", "noch", "wie",
    "einem", "über",
];

const PORTUGUESE_STOPWORDS: [&str; 40] = [
    "de", "a", "o", "que", "e", "do", "da", "em", "um", "para", "é", "com", "não", "uma", "os",
    "no", "se", "na", "por", "mais", "as", "dos", "como", "mas", "foi", "ao", "ele", "das", "tem",
    "à", "seu", "sua", "ou", "ser", "quando", "muito", "há", "nos", "já", "está",
];

const ITALIAN_STOPWORDS: [&str; 40] = [
    "di", "che", "è", "e", "la", "il", "un", "a", "per", "in", "una", "mi", "ho", "si", "lo", "ma",
    "ha", "cosa", "le", "con", "ti", "se", "no", "da", "non", "ci", "io", "questo", "come", "al",
    "del", "bene", "sei", "suo", "era", "sono", "qui", "sta", "anche", "dei",
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_language_name() {
        assert_eq!(Language::English.name(), "english");
        assert_eq!(Language::Spanish.name(), "spanish");
        assert_eq!(Language::French.name(), "french");
        assert_eq!(Language::German.name(), "german");
        assert_eq!(Language::Portuguese.name(), "portuguese");
        assert_eq!(Language::Italian.name(), "italian");
    }

    #[test]
    fn test_language_stopwords() {
        let sw = Language::English.stopwords();
        assert!(sw.contains(&"the"));
        assert!(sw.contains(&"and"));

        let sw_es = Language::Spanish.stopwords();
        assert!(sw_es.contains(&"de"));
        assert!(sw_es.contains(&"la"));
    }

    #[test]
    fn test_language_stopword_filter() {
        let filter = Language::English.stopword_filter();
        let tokens = vec![
            Token::new("the", 0, 3, 0),
            Token::new("quick", 4, 9, 1),
            Token::new("fox", 10, 13, 2),
        ];
        let filtered = filter.filter(tokens);
        assert_eq!(filtered.len(), 2);
        assert_eq!(filtered[0].text, "quick");
    }

    #[test]
    fn test_language_stemmer_english() {
        let stemmer = Language::English.stemmer_filter();
        let tokens = vec![Token::new("running", 0, 7, 0)];
        let stemmed = stemmer.filter(tokens);
        assert_eq!(stemmed[0].text, "runn");
    }

    #[test]
    fn test_language_stemmer_spanish() {
        let stemmer = Language::Spanish.stemmer_filter();
        let tokens = vec![Token::new("corriendo", 0, 9, 0)];
        let stemmed = stemmer.filter(tokens);
        // "corriendo" -> strips "iendo" -> "corr"
        assert_ne!(stemmed[0].text, "corriendo");
    }

    #[test]
    fn test_language_stemmer_french() {
        let stemmer = Language::French.stemmer_filter();
        let tokens = vec![Token::new("rapidement", 0, 10, 0)];
        let stemmed = stemmer.filter(tokens);
        assert_ne!(stemmed[0].text, "rapidement");
    }

    #[test]
    fn test_language_stemmer_german() {
        let stemmer = Language::German.stemmer_filter();
        let tokens = vec![Token::new("wanderungen", 0, 11, 0)];
        let stemmed = stemmer.filter(tokens);
        assert_ne!(stemmed[0].text, "wanderungen");
    }

    #[test]
    fn test_language_analyzer_english() {
        let analyzer = LanguageAnalyzer::new(Language::English);
        assert_eq!(analyzer.language(), Language::English);

        let tokens = analyzer.analyze("The quick brown foxes are jumping");
        // "the" and "are" are stopwords
        assert!(tokens.iter().all(|t| t.text != "the"));
        assert!(tokens.iter().all(|t| t.text != "are"));
    }

    #[test]
    fn test_language_analyzer_spanish() {
        let analyzer = LanguageAnalyzer::new(Language::Spanish);
        let tokens = analyzer.analyze("El rápido zorro marrón");
        // "el" is a stopword in Spanish
        assert!(tokens.iter().all(|t| t.text != "el"));
    }

    #[test]
    fn test_language_detector_default() {
        let detector = LanguageDetector::new(Language::English);
        // Very short text falls back to default
        let lang = detector.detect("hi");
        assert_eq!(lang, Language::English);
    }

    #[test]
    fn test_language_detector_english() {
        let detector = LanguageDetector::default();
        let lang = detector
            .detect("The quick brown fox jumps over the lazy dog and the cat sleeps on the mat");
        assert_eq!(lang, Language::English);
    }

    #[test]
    fn test_language_detector_spanish() {
        let detector = LanguageDetector::default();
        let lang = detector.detect(
            "El rápido zorro marrón salta porque quiere saltar sobre el perro perezoso que está en la casa",
        );
        assert_eq!(lang, Language::Spanish);
    }

    #[test]
    fn test_language_detector_german() {
        let detector = LanguageDetector::default();
        let lang = detector.detect(
            "Der schnelle braune Fuchs springt über den faulen Hund und die Katze schläft auf der Matte",
        );
        assert_eq!(lang, Language::German);
    }

    #[test]
    fn test_all_languages_have_stopwords() {
        let languages = [
            Language::English,
            Language::Spanish,
            Language::French,
            Language::German,
            Language::Portuguese,
            Language::Italian,
        ];
        for lang in &languages {
            assert!(
                !lang.stopwords().is_empty(),
                "{} should have stopwords",
                lang.name()
            );
        }
    }

    #[test]
    fn test_all_languages_have_stemmers() {
        let languages = [
            Language::English,
            Language::Spanish,
            Language::French,
            Language::German,
            Language::Portuguese,
            Language::Italian,
        ];
        for lang in &languages {
            let stemmer = lang.stemmer_filter();
            let tokens = vec![Token::new("testing", 0, 7, 0)];
            let result = stemmer.filter(tokens);
            assert!(
                !result.is_empty(),
                "{} stemmer should produce output",
                lang.name()
            );
        }
    }
}
