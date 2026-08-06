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
  "SECURITY.md#reporting-a-vulnerability",
  "security@ferritelabs.dev"
]
required_canonical_text.each do |text|
  fail("TESTER_PROGRAM.md is missing #{text.inspect}") unless canonical.downcase.include?(text.downcase)
end

{
  "README.md" => "[TESTER_PROGRAM.md](TESTER_PROGRAM.md)",
  "COMMUNITY.md" => "[Tester Program](./TESTER_PROGRAM.md)",
  "ADOPTERS.md" => "[Tester Program](TESTER_PROGRAM.md)"
}.each do |file, link|
  fail("#{file} must link to TESTER_PROGRAM.md") unless read(root, file).include?(link)
end

config = read(root, ".github/ISSUE_TEMPLATE/config.yml")
fail("issue config must link TESTER_PROGRAM.md") unless config.include?("/blob/main/TESTER_PROGRAM.md")
fail("issue config must link the canonical security policy") unless config.include?("SECURITY.md#reporting-a-vulnerability")
fail("issue config must list the security contact email") unless config.include?("security@ferritelabs.dev")

public_assets = {
  "TESTER_PROGRAM.md" => canonical,
  ".github/ISSUE_TEMPLATE/tester_interest.yml" => read(root, ".github/ISSUE_TEMPLATE/tester_interest.yml"),
  ".github/ISSUE_TEMPLATE/tester_report.yml" => read(root, ".github/ISSUE_TEMPLATE/tester_report.yml"),
  ".github/ISSUE_TEMPLATE/config.yml" => config
}

# These public-facing assets must never depend on GitHub Discussions or
# private Security Advisories (GitHub "Report a vulnerability"): both are
# disabled for this repository, so a link to either is a dead end for
# testers. The canonical channels are TESTER_PROGRAM.md/SECURITY.md and the
# tester interest issue form.
public_assets.each do |name, content|
  fail("#{name} must not link to GitHub Discussions (disabled for this repository)") if content.match?(%r{github\.com/[\w-]+/[\w.-]+/discussions})
  fail("#{name} must not link to private Security Advisories (disabled for this repository)") if content.match?(%r{security/advisories/new})
  fail("#{name} must use lowercase ferritelabs URLs") if content.match?(%r{github\.com/FerriteLabs})
  fail("#{name} must not reference a latest image") if content.match?(%r{ghcr\.io/ferritelabs/ferrite:latest})
  # No hardcoded campaign artifact: a concrete version tag (e.g. :0.4.0) or a
  # full 64-hex sha256 digest would silently violate the launch gate (the
  # campaign owner has not necessarily supplied one yet) and would go stale
  # the moment a real campaign image is issued. Illustrative placeholders
  # (e.g. "@sha256:..." or "<CAMPAIGN_IMAGE_DIGEST>") are allowed.
  fail("#{name} must not hardcode a concrete campaign image tag") if content.match?(%r{ghcr\.io/ferritelabs/ferrite:\d+\.\d+(\.\d+)?(?!\S)})
  fail("#{name} must not hardcode a concrete campaign image digest") if content.match?(%r{ghcr\.io/ferritelabs/ferrite@sha256:[0-9a-fA-F]{64}})
end

fail("SECURITY.md must exist") unless File.file?(File.join(root, "SECURITY.md"))
security_policy = read(root, "SECURITY.md")
fail("SECURITY.md must document the security contact email") unless security_policy.include?("security@ferritelabs.dev")

puts "Tester assets are valid."
RUBY
