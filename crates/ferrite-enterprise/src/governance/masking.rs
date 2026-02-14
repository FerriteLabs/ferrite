//! Data Masking and Redaction

use serde::{Deserialize, Serialize};

/// Masking rule definition
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum MaskingRule {
    /// Full redaction
    FullRedact,
    /// Partial mask (show first/last N characters)
    PartialMask {
        show_first: usize,
        show_last: usize,
        mask_char: char,
    },
    /// Email mask (user***@domain.com)
    EmailMask,
    /// Phone mask (***-***-1234)
    PhoneMask,
    /// Credit card mask (****-****-****-1234)
    CreditCardMask,
    /// SSN mask (***-**-1234)
    SsnMask,
    /// Hash the value
    Hash,
    /// Custom regex replacement
    Regex {
        pattern: String,
        replacement: String,
    },
    /// Tokenize (replace with token, store mapping)
    Tokenize,
    /// No masking
    None,
}

/// Masking strategy
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum MaskingStrategy {
    /// Always mask
    Always,
    /// Mask based on role
    RoleBased(Vec<String>),
    /// Mask based on classification
    ClassificationBased,
    /// Custom predicate
    Custom(String),
}

/// Redaction policy
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RedactionPolicy {
    /// Policy ID
    pub id: String,
    /// Key patterns
    pub patterns: Vec<String>,
    /// Masking rule
    pub rule: MaskingRule,
    /// Strategy
    pub strategy: MaskingStrategy,
    /// Enabled
    pub enabled: bool,
}

/// Masking engine
pub struct MaskingEngine {
    /// Policies
    policies: parking_lot::RwLock<Vec<RedactionPolicy>>,
}

impl MaskingEngine {
    /// Create a new masking engine
    pub fn new() -> Self {
        Self {
            policies: parking_lot::RwLock::new(Vec::new()),
        }
    }

    /// Apply masking rule to a value
    pub fn mask(&self, value: &str, rule: &MaskingRule) -> String {
        match rule {
            MaskingRule::FullRedact => "[REDACTED]".to_string(),
            MaskingRule::PartialMask {
                show_first,
                show_last,
                mask_char,
            } => self.partial_mask(value, *show_first, *show_last, *mask_char),
            MaskingRule::EmailMask => self.mask_email(value),
            MaskingRule::PhoneMask => self.mask_phone(value),
            MaskingRule::CreditCardMask => self.mask_credit_card(value),
            MaskingRule::SsnMask => self.mask_ssn(value),
            MaskingRule::Hash => self.hash_value(value),
            MaskingRule::Regex {
                pattern,
                replacement,
            } => self.regex_mask(value, pattern, replacement),
            MaskingRule::Tokenize => self.tokenize(value),
            MaskingRule::None => value.to_string(),
        }
    }

    fn partial_mask(
        &self,
        value: &str,
        show_first: usize,
        show_last: usize,
        mask_char: char,
    ) -> String {
        let len = value.len();
        if len <= show_first + show_last {
            return value.to_string();
        }

        let first: String = value.chars().take(show_first).collect();
        let last: String = value.chars().skip(len - show_last).collect();
        let middle = mask_char.to_string().repeat(len - show_first - show_last);

        format!("{}{}{}", first, middle, last)
    }

    fn mask_email(&self, value: &str) -> String {
        if let Some(at_pos) = value.find('@') {
            let (user, domain) = value.split_at(at_pos);
            let masked_user = if user.len() <= 2 {
                "*".repeat(user.len())
            } else {
                format!("{}***", &user[..1])
            };
            format!("{}{}", masked_user, domain)
        } else {
            self.partial_mask(value, 2, 0, '*')
        }
    }

    fn mask_phone(&self, value: &str) -> String {
        let digits: String = value.chars().filter(|c| c.is_ascii_digit()).collect();
        if digits.len() >= 4 {
            let last4 = &digits[digits.len() - 4..];
            format!("***-***-{}", last4)
        } else {
            "***-***-****".to_string()
        }
    }

    fn mask_credit_card(&self, value: &str) -> String {
        let digits: String = value.chars().filter(|c| c.is_ascii_digit()).collect();
        if digits.len() >= 4 {
            let last4 = &digits[digits.len() - 4..];
            format!("****-****-****-{}", last4)
        } else {
            "****-****-****-****".to_string()
        }
    }

    fn mask_ssn(&self, value: &str) -> String {
        let digits: String = value.chars().filter(|c| c.is_ascii_digit()).collect();
        if digits.len() >= 4 {
            let last4 = &digits[digits.len() - 4..];
            format!("***-**-{}", last4)
        } else {
            "***-**-****".to_string()
        }
    }

    fn hash_value(&self, value: &str) -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        format!("{:016x}", hasher.finish())
    }

    fn regex_mask(&self, _value: &str, _pattern: &str, replacement: &str) -> String {
        // Simplified: in production, use regex crate
        replacement.to_string()
    }

    fn tokenize(&self, value: &str) -> String {
        // Generate a token (in production, store mapping)
        format!("TOKEN_{}", &self.hash_value(value)[..8])
    }

    /// Add a redaction policy
    pub fn add_policy(&self, policy: RedactionPolicy) {
        self.policies.write().push(policy);
    }

    /// Get masking rule for a key
    pub fn get_rule_for_key(&self, key: &str) -> Option<MaskingRule> {
        for policy in self.policies.read().iter() {
            if !policy.enabled {
                continue;
            }
            for pattern in &policy.patterns {
                if self.matches_pattern(pattern, key) {
                    return Some(policy.rule.clone());
                }
            }
        }
        None
    }

    fn matches_pattern(&self, pattern: &str, key: &str) -> bool {
        if pattern == "*" {
            return true;
        }
        if let Some(prefix) = pattern.strip_suffix('*') {
            return key.starts_with(prefix);
        }
        pattern == key
    }
}

impl Default for MaskingEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_full_redact() {
        let engine = MaskingEngine::new();
        let result = engine.mask("sensitive data", &MaskingRule::FullRedact);
        assert_eq!(result, "[REDACTED]");
    }

    #[test]
    fn test_partial_mask() {
        let engine = MaskingEngine::new();
        let result = engine.mask(
            "1234567890",
            &MaskingRule::PartialMask {
                show_first: 2,
                show_last: 2,
                mask_char: '*',
            },
        );
        assert_eq!(result, "12******90");
    }

    #[test]
    fn test_email_mask() {
        let engine = MaskingEngine::new();
        let result = engine.mask("john.doe@example.com", &MaskingRule::EmailMask);
        assert!(result.contains("***"));
        assert!(result.contains("@example.com"));
    }

    #[test]
    fn test_phone_mask() {
        let engine = MaskingEngine::new();
        let result = engine.mask("555-123-4567", &MaskingRule::PhoneMask);
        assert_eq!(result, "***-***-4567");
    }

    #[test]
    fn test_credit_card_mask() {
        let engine = MaskingEngine::new();
        let result = engine.mask("4111-1111-1111-1234", &MaskingRule::CreditCardMask);
        assert_eq!(result, "****-****-****-1234");
    }

    #[test]
    fn test_ssn_mask() {
        let engine = MaskingEngine::new();
        let result = engine.mask("123-45-6789", &MaskingRule::SsnMask);
        assert_eq!(result, "***-**-6789");
    }

    #[test]
    fn test_hash() {
        let engine = MaskingEngine::new();
        let result = engine.mask("test", &MaskingRule::Hash);
        assert_eq!(result.len(), 16);
    }
}
