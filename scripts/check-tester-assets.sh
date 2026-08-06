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

required_forms.each do |filename, required_ids|
  form = load_form(root, filename)
  fail("#{filename} must be a mapping") unless form.is_a?(Hash)
  %w[name description body].each do |key|
    fail("#{filename} is missing #{key}") unless form.key?(key)
  end

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
  "exact immutable artifact reference",
  "never use `latest`",
  "Redis/client compatibility",
  "Durability/restart",
  "Operations/metrics",
  "Performance comparison",
  "IDE tooling",
  "./scripts/tester.sh start",
  "./scripts/tester.sh smoke",
  "./scripts/tester.sh durability",
  "./scripts/tester.sh diagnostics",
  "./scripts/tester.sh stop",
  "./scripts/tester.sh reset",
  "template=tester_interest.yml",
  "template=tester_report.yml",
  "docs/FEATURE_MATURITY.md",
  "three business days",
  "8–12 testers"
]
required_canonical_text.each do |text|
  fail("TESTER_PROGRAM.md is missing #{text.inspect}") unless canonical.include?(text)
end

{
  "README.md" => "[TESTER_PROGRAM.md](TESTER_PROGRAM.md)",
  "COMMUNITY.md" => "[Tester Program](./TESTER_PROGRAM.md)",
  "ADOPTERS.md" => "[Tester Program](TESTER_PROGRAM.md)"
}.each do |file, link|
  fail("#{file} must link to TESTER_PROGRAM.md") unless read(root, file).include?(link)
end

config = read(root, ".github/ISSUE_TEMPLATE/config.yml")
fail("issue config must preserve security advisories") unless config.include?("/security/advisories/new")
fail("issue config must preserve Discussions") unless config.include?("/discussions")
fail("issue config must link TESTER_PROGRAM.md") unless config.include?("/blob/main/TESTER_PROGRAM.md")

public_assets = [
  canonical,
  read(root, ".github/ISSUE_TEMPLATE/tester_interest.yml"),
  read(root, ".github/ISSUE_TEMPLATE/tester_report.yml"),
  config
].join("\n")
fail("public tester assets must use lowercase ferritelabs URLs") if public_assets.match?(%r{github\.com/FerriteLabs})
fail("public tester assets must not reference a latest image") if public_assets.match?(%r{ghcr\.io/ferritelabs/ferrite:latest})

puts "Tester assets are valid."
RUBY
