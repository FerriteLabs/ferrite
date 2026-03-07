//! WASM Playground example
//!
//! Demonstrates using the Ferrite playground as a standalone REPL
//! for interactive exploration of Ferrite commands.
//!
//! Run: cargo run --example wasm_playground

fn main() {
    println!("=== Ferrite WASM Playground ===\n");

    let mut playground = ferrite::wasm::playground::PlaygroundInstance::new();

    // Load tutorial data (sets up sample keys, hashes, sorted sets, etc.)
    playground.load_tutorial_data();
    println!("Tutorial data loaded.\n");

    // Basic key-value operations
    println!("--- Basic Operations ---");
    let basic_commands = vec![
        "PING",
        "SET greeting \"Hello, Ferrite!\"",
        "GET greeting",
        "SET counter 0",
        "INCR counter",
        "INCR counter",
        "INCR counter",
        "GET counter",
    ];

    for cmd in &basic_commands {
        println!("> {}", cmd);
        println!("{}\n", playground.execute_command(cmd));
    }

    // Data structure operations
    println!("--- Data Structures ---");
    let ds_commands = vec![
        "HSET user:1 name Alice age 30 city Portland",
        "HGETALL user:1",
        "LPUSH tasks \"Write docs\" \"Fix bugs\" \"Ship feature\"",
        "LRANGE tasks 0 -1",
        "SADD tags rust database cache redis",
        "SMEMBERS tags",
        "ZADD leaderboard 100 alice 85 bob 92 charlie",
        "ZRANGE leaderboard 0 -1 WITHSCORES",
    ];

    for cmd in &ds_commands {
        println!("> {}", cmd);
        println!("{}\n", playground.execute_command(cmd));
    }

    // Server inspection
    println!("--- Server Inspection ---");
    let inspect_commands = vec!["DBSIZE", "KEYS *", "INFO"];

    for cmd in &inspect_commands {
        println!("> {}", cmd);
        let output = playground.execute_command(cmd);
        // Truncate INFO output for readability
        if cmd == &"INFO" {
            let lines: Vec<&str> = output.lines().take(10).collect();
            println!("{}\n  ... (truncated)\n", lines.join("\n"));
        } else {
            println!("{}\n", output);
        }
    }

    // Batch execution
    println!("--- Batch Execution ---");
    let batch = vec![
        "SET batch:a 1",
        "SET batch:b 2",
        "SET batch:c 3",
        "MGET batch:a batch:b batch:c",
    ];
    let results = playground.execute_batch(&batch);
    for (cmd, result) in batch.iter().zip(results.iter()) {
        println!("> {} → {}", cmd, result);
    }

    // Reset and verify
    println!("\n--- Reset ---");
    playground.reset();
    println!("> DBSIZE after reset");
    println!("{}", playground.execute_command("DBSIZE"));

    // Final stats
    println!("\n--- Playground Stats ---");
    let stats = playground.stats();
    println!("{:?}", stats);
}
