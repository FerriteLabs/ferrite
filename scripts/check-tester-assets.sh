#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

command -v ruby >/dev/null 2>&1 || {
  echo "error: Ruby is required to validate tester issue forms" >&2
  exit 1
}

ruby - "$ROOT" <<'RUBY'
require "yaml"

root = ARGV.fetch(0)

def fail(message)
  warn "tester-assets: #{message}"
  exit 1
end

def read(root, relative)
  path = File.join(root, relative)
  fail("missing #{relative}") unless File.file?(path)
  File.read(path, encoding: "UTF-8")
end

def load_form(root, filename)
  relative = File.join(".github", "ISSUE_TEMPLATE", filename)
  YAML.safe_load(read(root, relative), permitted_classes: [], permitted_symbols: [], aliases: false)
rescue Psych::Exception => error
  fail("#{relative} is invalid YAML: #{error.message}")
end

# GitHub reserves certain dropdown option text (e.g. "None") because it can be
# confused with an unset/no-selection value in the rendered issue and in
# automation that parses submitted forms. Reject these regardless of case, and
# separately require every dropdown's options to be non-empty and unique so a
# copy/paste mistake can't silently duplicate or blank an option.
RESERVED_DROPDOWN_OPTIONS = %w[none n/a null undefined].freeze

def validate_dropdown_options!(filename, field)
  return unless field["type"] == "dropdown"

  field_id = field["id"] || "<unknown>"
  options = field.dig("attributes", "options")
  fail("#{filename} dropdown #{field_id} must declare a non-empty options list") unless options.is_a?(Array) && !options.empty?

  seen = {}
  options.each do |option|
    fail("#{filename} dropdown #{field_id} options must be strings") unless option.is_a?(String)
    text = option
    fail("#{filename} dropdown #{field_id} has a blank option") if text.strip.empty?

    normalized = text.strip.downcase
    fail("#{filename} dropdown #{field_id} has duplicate option #{text.inspect}") if seen[normalized]
    seen[normalized] = true

    if RESERVED_DROPDOWN_OPTIONS.include?(normalized)
      fail("#{filename} dropdown #{field_id} uses GitHub-reserved option #{text.inspect}; use an explicit label instead (e.g. \"No issues observed\")")
    end
  end
end

required_forms = {
  "tester_interest.yml" => %w[environment client track availability safety],
  "tester_report.yml" => %w[
    track severity version_image install_method environment client steps expected
    actual reproducibility regression redaction
  ]
}

# Exact, canonical form names/titles. These must not drift silently because
# other assets (config.yml contact links, TESTER_PROGRAM.md prose, launch
# checklists) reference them by exact template filename and title.
expected_form_metadata = {
  "tester_interest.yml" => {
    "name" => "External tester interest",
    "title" => "[Tester interest] "
  },
  "tester_report.yml" => {
    "name" => "External tester report",
    "title" => "[Tester report] "
  }
}

expected_docker_only_options = {
  "tester_interest.yml" => {
    "environment" => ["Docker / Docker Compose"]
  },
  "tester_report.yml" => {
    "install_method" => ["ferrite-ops tester Docker Compose"]
  }
}

required_forms.each do |filename, required_ids|
  form = load_form(root, filename)
  fail("#{filename} must be a mapping") unless form.is_a?(Hash)
  %w[name description body].each do |key|
    fail("#{filename} is missing #{key}") unless form.key?(key)
  end

  expected_form_metadata.fetch(filename).each do |key, expected_value|
    actual_value = form[key]
    unless actual_value == expected_value
      fail("#{filename} #{key} must be exactly #{expected_value.inspect}, got #{actual_value.inspect}")
    end
  end

  # Issue forms must not reference labels that do not exist in this
  # repository (e.g. a never-created "tester-program"/"triage" label);
  # GitHub silently drops unknown labels, which misleads maintainers who
  # expect issues to be pre-labeled.
  fail("#{filename} must not declare a labels list") if form.key?("labels")

  body = form["body"]
  fail("#{filename} body must be a non-empty list") unless body.is_a?(Array) && !body.empty?
  fields = body.reject { |item| item["type"] == "markdown" }
  ids = fields.map { |item| item["id"] }
  fail("#{filename} contains a field without an id") if ids.any? { |id| id.nil? || id.empty? }
  fail("#{filename} contains duplicate field ids") unless ids.uniq.length == ids.length

  fields.each do |field|
    validate_dropdown_options!(filename, field)
    expected_options = expected_docker_only_options.fetch(filename, {})[field["id"]]
    next unless expected_options

    actual_options = field.dig("attributes", "options")
    unless actual_options == expected_options
      fail("#{filename} dropdown #{field['id']} must contain only the initial Docker/Compose cohort option")
    end
  end

  missing = required_ids - ids
  fail("#{filename} is missing fields: #{missing.join(', ')}") unless missing.empty?

  required_ids.each do |id|
    field = fields.find { |item| item["id"] == id }
    required =
      if field["type"] == "checkboxes"
        options = field.dig("attributes", "options")
        options.is_a?(Array) && !options.empty? &&
          options.all? { |option| option["required"] == true }
      else
        field.dig("validations", "required") == true
      end
    fail("#{filename} field #{id} must be required") unless required
  end
end

canonical = read(root, "TESTER_PROGRAM.md")
required_canonical_text = [
  "60–90 minutes",
  "Launch gate",
  "CAMPAIGN_OPS_REF",
  "FERRITE_TEST_IMAGE",
  "CAMPAIGN_IMAGE_DIGEST",
  "never use `latest`",
  "Redis/client compatibility",
  "Durability/restart",
  "Operations/metrics",
  "Performance comparison",
  "IDE tooling",
  "git checkout <CAMPAIGN_OPS_REF>",
  "./scripts/tester.sh start",
  "./scripts/tester.sh smoke",
  "./scripts/tester.sh durability",
  "./scripts/tester.sh diagnostics",
  "./scripts/tester.sh stop",
  "./scripts/tester.sh reset",
  "FERRITE_TEST_ENABLE_DURABILITY=1",
  "template=tester_interest.yml",
  "template=tester_report.yml",
  "docs/FEATURE_MATURITY.md",
  "three business days",
  "8–12 testers",
  "No issues observed",
  "SECURITY.md#reporting-a-vulnerability",
  "https://github.com/ferritelabs/ferrite/security/advisories/new"
]
required_canonical_text.each do |text|
  fail("TESTER_PROGRAM.md is missing #{text.inspect}") unless canonical.downcase.include?(text.downcase)
end

# The initial tester cohort is Docker/Docker Compose only. Homebrew, source
# builds, and Kubernetes are deferred until maintained tooling exists for
# them, so the canonical program document must not present them as available
# install paths for the current cohort (mentioning them only as explicitly
# "deferred" alternatives is fine and is not matched by this pattern).
fail("TESTER_PROGRAM.md must not present Homebrew as an available install path for the initial cohort") if canonical.match?(/homebrew formula/i)
fail("TESTER_PROGRAM.md must not present a source build as an available install path for the initial cohort") if canonical.match?(/source commit.*built clean/i)

{
  "README.md" => "[TESTER_PROGRAM.md](TESTER_PROGRAM.md)",
  "COMMUNITY.md" => "[Tester Program](./TESTER_PROGRAM.md)",
  "ADOPTERS.md" => "[Tester Program](TESTER_PROGRAM.md)"
}.each do |file, link|
  fail("#{file} must link to TESTER_PROGRAM.md") unless read(root, file).include?(link)
end

config = read(root, ".github/ISSUE_TEMPLATE/config.yml")
fail("issue config must link TESTER_PROGRAM.md") unless config.include?("/blob/main/TESTER_PROGRAM.md")
fail("issue config must link GitHub private vulnerability reporting") unless config.include?("https://github.com/ferritelabs/ferrite/security/advisories/new")

public_assets = {
  "TESTER_PROGRAM.md" => canonical,
  "COMMUNITY.md" => read(root, "COMMUNITY.md"),
  ".github/ISSUE_TEMPLATE/tester_interest.yml" => read(root, ".github/ISSUE_TEMPLATE/tester_interest.yml"),
  ".github/ISSUE_TEMPLATE/tester_report.yml" => read(root, ".github/ISSUE_TEMPLATE/tester_report.yml"),
  ".github/ISSUE_TEMPLATE/config.yml" => config
}

private_reporting_url = "https://github.com/ferritelabs/ferrite/security/advisories/new"

# GitHub private vulnerability reporting (Security Advisories) is the sole
# canonical private security intake for this repository. These public-facing
# assets must never resurrect the retired email/PGP/mailing-list channels or
# claim that private reporting is unavailable/disabled, and must never depend
# on GitHub Discussions, which is not enabled for this repository.
public_assets.each do |name, content|
  fail("#{name} must link to the canonical GitHub private vulnerability reporting intake") unless content.include?(private_reporting_url)
  fail("#{name} must not link to GitHub Discussions (not enabled for this repository)") if content.match?(%r{github\.com/[\w-]+/[\w.-]+/discussions})
  fail("#{name} must not reference the retired security@ferritelabs.dev email intake") if content.match?(/security@ferritelabs\.dev/i)
  fail("#{name} must not reference a PGP key for security reporting") if content.match?(/\bPGP\b/i)
  fail("#{name} must not reference a security mailing list") if content.match?(/security mailing list/i)
  fail("#{name} must not describe private vulnerability reporting as disabled/unavailable") if content.match?(/private vulnerability reporting[^.]*(not currently enabled|is not enabled|not enabled|disabled)/i)
  fail("#{name} must use lowercase ferritelabs URLs") if content.match?(%r{github\.com/FerriteLabs})
  fail("#{name} must describe a candidate/hardening campaign, not a pre-release") if content.match?(/\bpre-release\b/i)
  fail("#{name} must not reference a latest image") if content.match?(%r{ghcr\.io/ferritelabs/ferrite:latest})
end

# --- Concrete campaign image detection -------------------------------------
#
# No hardcoded campaign artifact: a concrete tag (e.g. `:v0.4.0`, `:0.4.0`, or
# a named candidate tag like `:release-candidate-1`) or a full 64-hex sha256
# digest would silently violate the launch gate (the campaign owner has not
# necessarily supplied one yet) and would go stale the moment a real campaign
# image is issued. Illustrative placeholders that begin with `<` (e.g.
# `<CAMPAIGN_IMAGE_TAG>`, `<CAMPAIGN_IMAGE_DIGEST>`) are allowed, as is the
# literal digest placeholder prose `sha256:...`.
#
# The tag pattern is intentionally broad (matches Markdown backticks, plain
# prose, and end-of-line) so it can't be defeated by wrapping a real tag in
# formatting the earlier narrower `\d+\.\d+` regex would have missed (e.g.
# `v0.4.0` or a named candidate tag).
def concrete_image_violation(text)
  text.scan(%r{ghcr\.io/ferritelabs/ferrite:([^\s`"'()\[\]]+)}) do |(tag)|
    next if tag.start_with?("<")
    return "a concrete campaign image tag (#{tag.inspect})" if tag.match?(/\A[A-Za-z0-9]/)
  end

  if text.match?(/sha256:[0-9a-fA-F]{64}/)
    return "a concrete campaign image digest"
  end

  nil
end

# Deterministic self-test coverage for the detector above. Runs on every
# invocation (including CI) so a future edit to the detector regexes cannot
# silently regress without failing the very next run.
def self_test_concrete_image_detection!
  cases = {
    "plain semver tag" => ["Use ghcr.io/ferritelabs/ferrite:0.4.0 for this campaign.", true],
    "v-prefixed tag" => ["Use ghcr.io/ferritelabs/ferrite:v0.4.0 for this campaign.", true],
    "named candidate tag" => ["Use ghcr.io/ferritelabs/ferrite:release-candidate-1 for this campaign.", true],
    "markdown backtick tag" => ["Run `ghcr.io/ferritelabs/ferrite:v0.4.0` locally.", true],
    "full sha256 digest" => ["Use ghcr.io/ferritelabs/ferrite@sha256:#{'a' * 64} for this campaign.", true],
    "bare sha256 digest" => ["Digest: sha256:#{'f' * 64}", true],
    "tag placeholder" => ["Use ghcr.io/ferritelabs/ferrite:<CAMPAIGN_IMAGE_TAG> for this campaign.", false],
    "digest placeholder" => ["Use ghcr.io/ferritelabs/ferrite@<CAMPAIGN_IMAGE_DIGEST> for this campaign.", false],
    "digest prose placeholder" => ["Digest: sha256:...", false],
    "latest tag is a separate check" => ["Use ghcr.io/ferritelabs/ferrite:latest for this campaign.", true]
  }

  cases.each do |label, (text, expect_violation)|
    violation = concrete_image_violation(text)
    actual = !violation.nil?
    unless actual == expect_violation
      fail("self-test failed for #{label.inspect}: expected violation=#{expect_violation}, got #{actual.inspect} (#{violation})")
    end
  end
end

self_test_concrete_image_detection!

public_assets.each do |name, content|
  violation = concrete_image_violation(content)
  fail("#{name} must not hardcode #{violation}") if violation
end

fail("SECURITY.md must exist") unless File.file?(File.join(root, "SECURITY.md"))
security_policy = read(root, "SECURITY.md")
fail("SECURITY.md must document GitHub private vulnerability reporting as the canonical channel") unless security_policy.include?("https://github.com/ferritelabs/ferrite/security/advisories/new")
fail("SECURITY.md must not reference the retired security@ferritelabs.dev email intake") if security_policy.match?(/security@ferritelabs\.dev/i)
fail("SECURITY.md must not reference a PGP key for security reporting") if security_policy.match?(/\bPGP\b/i)
fail("SECURITY.md must not reference a security mailing list") if security_policy.match?(/security mailing list/i)

fail("security_vulnerability.md issue template must be removed now that GitHub private vulnerability reporting is canonical") if File.file?(File.join(root, ".github", "ISSUE_TEMPLATE", "security_vulnerability.md"))

puts "Tester assets are valid."
RUBY
