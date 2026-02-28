//! WASM Playground example
//!
//! Demonstrates using the Ferrite playground as a standalone REPL.
//! Run: cargo run --example wasm_playground

fn main() {
    let mut playground = ferrite::wasm::playground::PlaygroundInstance::new();
    playground.load_tutorial_data();

    // Demo commands
    let commands = vec![
        "PING",
        "SET greeting \"Hello, Ferrite!\"",
        "GET greeting",
        "KEYS *",
        "DBSIZE",
        "HGETALL config:app",
        "ZRANGE leaderboard 0 -1 WITHSCORES",
        "INFO",
    ];

    for cmd in commands {
        println!("> {}", cmd);
        println!("{}", playground.execute_command(cmd));
        println!();
    }

    println!("Stats: {:?}", playground.stats());
}
