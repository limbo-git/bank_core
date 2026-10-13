//! 对账工具 CLI:对账 → 打印摘要 → 有违规即非零退出(可接 CI/定时任务)。
//!
//! 用法:`BANK_DB_* 环境变量就绪后` cargo run -p bank-reconciler [-- --json]

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let json_mode = std::env::args().any(|a| a == "--json");
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .connect(&bank_reconciler::bank_db_url()?)
        .await?;

    let report = bank_reconciler::reconcile(&pool).await?;

    if json_mode {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        println!("=== 对账报告 ===");
        println!(
            "统计: 命令 {} (已提交 {}) / 分录 {} / 账户 {} / 悬挂命令 {}",
            report.stats.commands,
            report.stats.committed_commands,
            report.stats.entries,
            report.stats.accounts,
            report.stats.dangling_commands,
        );
        println!("检查项: {}", report.checks_run.join(" | "));
        if report.is_clean() {
            println!("结论: 账目一致 ✅ (0 违规)");
        } else {
            println!("结论: 发现 {} 项不一致 ❌", report.violations.len());
            for v in &report.violations {
                println!("  [{}] {}", v.check, v.detail);
            }
        }
    }

    if report.is_clean() {
        Ok(())
    } else {
        std::process::exit(1);
    }
}
