//! 崩溃跑批辅助程序(M3 的 kill -9 测试用):持续对不同账户做转账,直到被强杀。
//!
//! 用法(环境变量与 bank-api 相同):
//!   crash_runner --from <账号> --to <账号> [--iters N] [--amount-cents N]

use std::env;
use std::process::exit;

use bank_domain::{Command, Transfer};
use bank_store::commands::execute_command;

fn arg(name: &str) -> Option<String> {
    let args: Vec<String> = env::args().collect();
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1).cloned())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let from = arg("--from").unwrap_or_else(|| {
        eprintln!("缺少 --from");
        exit(2)
    });
    let to = arg("--to").unwrap_or_else(|| {
        eprintln!("缺少 --to");
        exit(2)
    });
    let iters: u64 = arg("--iters")
        .and_then(|v| v.parse().ok())
        .unwrap_or(u64::MAX);
    let amount: i64 = arg("--amount-cents")
        .and_then(|v| v.parse().ok())
        .unwrap_or(100);

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .connect(&bank_store::bank_db_url()?)
        .await?;

    for i in 0..iters {
        let key = format!("crash-{i}-{}", std::process::id());
        let cmd = Command::Transfer(Transfer::new(from.clone(), to.clone(), amount)?);
        match execute_command(&pool, &key, &cmd).await {
            Ok(out) => println!("{key} committed replay={}", out.replay),
            Err(e) => println!("{key} failed: {e}"),
        }
    }
    Ok(())
}
