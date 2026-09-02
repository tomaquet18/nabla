// SPDX-License-Identifier: MIT OR Apache-2.0
//! `follow [--rows] <connection-string> <view>`: print the snapshot, then one
//! block per source transaction, and every resync. Exit on Ctrl-C.

use std::process::ExitCode;

use nabla_client::{Event, Op, Subscription};

#[tokio::main]
async fn main() -> ExitCode {
    let mut args: Vec<String> = std::env::args().skip(1).collect();
    let print_rows = args.iter().any(|a| a == "--rows");
    args.retain(|a| a != "--rows");
    let (config, view) = match args.as_slice() {
        [config, view] => (config.clone(), view.clone()),
        _ => {
            eprintln!("usage: follow [--rows] <connection-string> <view>");
            return ExitCode::from(2);
        }
    };

    let mut sub = match Subscription::open(&config, &view).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("error: {e}");
            return ExitCode::from(1);
        }
    };

    loop {
        let event = tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                println!("interrupted");
                return ExitCode::SUCCESS;
            }
            event = sub.next() => event,
        };
        match event {
            Ok(Event::Snapshot { epoch, frontier, cursor, rows }) => {
                println!("snapshot: rows={} epoch={epoch} frontier={frontier} cursor={cursor}", rows.len());
                if print_rows {
                    for row in &rows {
                        println!("= {row}");
                    }
                }
            }
            Ok(Event::Transaction { xid, lsn, deltas, .. }) => {
                let xid = xid.map_or("-".to_string(), |x| x.to_string());
                println!("tx lsn={lsn} xid={xid} deltas={}", deltas.len());
                for d in &deltas {
                    let sign = match d.op {
                        Op::Insert => '+',
                        Op::Delete => '-',
                    };
                    println!("  {} {sign}{}", d.seq, d.row);
                }
            }
            Ok(Event::Resync { reason }) => println!("resync: {reason}"),
            Err(e) => {
                eprintln!("error: {e}");
                return ExitCode::from(1);
            }
        }
    }
}
