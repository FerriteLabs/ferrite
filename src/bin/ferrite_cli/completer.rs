//! Tab completion support for ferrite-cli

use rustyline::completion::{Completer, Pair};
use rustyline::error::ReadlineError;
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::validate::Validator;
use rustyline::{Context, Helper};

use crate::commands::CommandRegistry;

/// Helper for rustyline with tab completion
pub struct FerriteHelper {
    registry: CommandRegistry,
}

impl FerriteHelper {
    pub fn new() -> Self {
        Self {
            registry: CommandRegistry::new(),
        }
    }
}

impl Completer for FerriteHelper {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &Context<'_>,
    ) -> Result<(usize, Vec<Pair>), ReadlineError> {
        let line = &line[..pos];
        let parts: Vec<&str> = line.split_whitespace().collect();

        // If we're at the start or completing the first word, complete commands
        if parts.is_empty() || (parts.len() == 1 && !line.ends_with(' ')) {
            let prefix = parts.first().copied().unwrap_or("");
            let completions = self.registry.get_completions(prefix);

            let candidates: Vec<Pair> = completions
                .into_iter()
                .map(|name| Pair {
                    display: name.to_string(),
                    replacement: name.to_string(),
                })
                .collect();

            // Find the start of the word being completed
            let start = line.rfind(char::is_whitespace).map(|i| i + 1).unwrap_or(0);
            Ok((start, candidates))
        } else {
            // For arguments, we could potentially complete key names
            // For now, return empty
            Ok((pos, vec![]))
        }
    }
}

impl Hinter for FerriteHelper {
    type Hint = String;

    fn hint(&self, line: &str, pos: usize, _ctx: &Context<'_>) -> Option<String> {
        if pos < line.len() {
            return None;
        }

        let parts: Vec<&str> = line.split_whitespace().collect();

        // Show command hint if completing first word
        if parts.len() == 1 && !line.ends_with(' ') {
            let prefix = parts[0];
            let completions = self.registry.get_completions(prefix);

            if completions.len() == 1 {
                let cmd = completions[0];
                let suffix = &cmd[prefix.len()..];
                return Some(suffix.to_string());
            }
        }

        // Show argument hints after command
        if !parts.is_empty() && line.ends_with(' ') {
            if let Some(info) = self.registry.get_command(parts[0]) {
                let hint = format!(" # {}", info.summary);
                return Some(hint);
            }
        }

        None
    }
}

impl Highlighter for FerriteHelper {}

impl Validator for FerriteHelper {}

impl Helper for FerriteHelper {}
