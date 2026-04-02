//! `ferrite-fn` — Forge function development CLI.
//!
//! A convenience wrapper around common Forge function operations:
//! scaffolding new projects, building WASM components, and deploying
//! them to a running Ferrite instance.
//!
//! # Usage
//!
//! ```text
//! ferrite-fn new <name> [--lang rust|go|ts]   Create a new function project
//! ferrite-fn build                            Build the current function project
//! ferrite-fn deploy <addr> <name> [path]      Deploy to a running Ferrite instance
//! ferrite-fn list <addr>                      List loaded functions
//! ferrite-fn help                             Show this help
//! ```
#![allow(clippy::print_stdout, clippy::print_stderr, clippy::unwrap_used)]

use std::env;
use std::process;

const VERSION: &str = env!("CARGO_PKG_VERSION");

fn print_help() {
    println!(
        "\
ferrite-fn {VERSION} — Forge function development CLI

USAGE:
    ferrite-fn <COMMAND> [OPTIONS]

COMMANDS:
    new <name> [--lang rust|go|ts]   Create a new function project from template
    build                            Build the current function (cargo component build)
    deploy <addr> <name> [path]      Deploy a function to a running Ferrite instance
    list <addr>                      List functions loaded on a Ferrite instance
    help                             Show this help message

EXAMPLES:
    ferrite-fn new my-filter
    ferrite-fn new my-filter --lang go
    ferrite-fn build
    ferrite-fn deploy localhost:6379 my-filter target/wasm32-wasip2/release/my_filter.wasm
    ferrite-fn list localhost:6379"
    );
}

fn cmd_new(args: &[String]) {
    if args.is_empty() {
        eprintln!("Error: missing function name");
        eprintln!("Usage: ferrite-fn new <name> [--lang rust|go|ts]");
        process::exit(1);
    }

    let name = &args[0];
    let lang = args
        .iter()
        .position(|a| a == "--lang")
        .and_then(|i| args.get(i + 1))
        .map(String::as_str)
        .unwrap_or("rust");

    match lang {
        "rust" => {
            println!("→ Would scaffold Rust function project '{name}'");
            println!();
            println!("  Manual equivalent:");
            println!("    cargo generate --path sdk/forge-templates/rust --name {name}");
            println!();
            println!("  Then build with:");
            println!("    cd {name} && cargo component build --release");
        }
        "go" => {
            println!("→ Would scaffold Go function project '{name}'");
            println!();
            println!("  Manual equivalent:");
            println!("    cp -r sdk/forge-templates/go {name}");
            println!();
            println!("  Then build with:");
            println!("    cd {name} && tinygo build -o function.wasm -target=wasip2 main.go");
        }
        "ts" => {
            println!("→ Would scaffold TypeScript function project '{name}'");
            println!();
            println!("  Manual equivalent:");
            println!("    cp -r sdk/forge-templates/ts {name} && cd {name} && npm install");
            println!();
            println!("  Then build with:");
            println!("    npm run build");
        }
        other => {
            eprintln!("Error: unsupported language '{other}'");
            eprintln!("Supported: rust, go, ts");
            process::exit(1);
        }
    }
}

fn cmd_build() {
    println!("→ Would build the current Forge function project");
    println!();
    println!("  Manual equivalent:");
    println!("    cargo component build --release");
    println!();
    println!("  Output: target/wasm32-wasip2/release/<name>.wasm");
}

fn cmd_deploy(args: &[String]) {
    if args.len() < 2 {
        eprintln!("Error: missing required arguments");
        eprintln!("Usage: ferrite-fn deploy <addr> <name> [path]");
        process::exit(1);
    }

    let addr = &args[0];
    let name = &args[1];
    let path = args.get(2).map_or_else(
        || {
            format!(
                "target/wasm32-wasip2/release/{}.wasm",
                name.replace('-', "_")
            )
        },
        Clone::clone,
    );

    println!("→ Would deploy function '{name}' to {addr}");
    println!("  WASM path: {path}");
    println!();
    println!("  Manual equivalent:");
    println!("    redis-cli -h {addr} FN.LOAD {name} /path/to/{path}");
}

fn cmd_list(args: &[String]) {
    if args.is_empty() {
        eprintln!("Error: missing server address");
        eprintln!("Usage: ferrite-fn list <addr>");
        process::exit(1);
    }

    let addr = &args[0];
    println!("→ Would list functions loaded on {addr}");
    println!();
    println!("  Manual equivalent:");
    println!("    redis-cli -h {addr} FN.LIST");
}

fn main() {
    let args: Vec<String> = env::args().skip(1).collect();

    if args.is_empty() {
        print_help();
        return;
    }

    match args[0].as_str() {
        "new" => cmd_new(&args[1..]),
        "build" => cmd_build(),
        "deploy" => cmd_deploy(&args[1..]),
        "list" => cmd_list(&args[1..]),
        "help" | "--help" | "-h" => print_help(),
        other => {
            eprintln!("Error: unknown command '{other}'");
            eprintln!();
            print_help();
            process::exit(1);
        }
    }
}
