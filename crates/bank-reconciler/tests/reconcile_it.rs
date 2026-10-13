//! T-D 对账工具自身的验证:正常账目干净;人为注入错误必须被抓出。
//! 这直接对应方案 §6 的 "T-D3 错误注入演练"。

use bank_domain::{Command, Deposit, OpenAccount};
use bank_store::commands::execute_command;
use sqlx::PgPool;

fn unique(tag: &str) -> String {
    format!(
        "{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

async fn pools() -> Option<(PgPool, PgPool)> {
    let host = std::env::var("BANK_DB_HOST").ok()?;
    let port = std::env::var("BANK_DB_PORT").ok()?;
    let name = std::env::var("BANK_DB_NAME").ok()?;
    let user = std::env::var("BANK_DB_USER").ok()?;
    let password = std::env::var("BANK_DB_PASSWORD").ok()?;
    let owner_user = std::env::var("BANK_DB_OWNER_USER").ok()?;
    let owner_password = std::env::var("BANK_DB_OWNER_PASSWORD").ok()?;
    let test_name = format!("{name}_test");

    let owner = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&format!(
            "postgres://{owner_user}:{owner_password}@{host}:{port}/{test_name}"
        ))
        .await
        .ok()?;
    bank_store::migrate(&owner).await.ok()?;

    let app = sqlx::postgres::PgPoolOptions::new()
        .max_connections(8)
        .connect(&format!(
            "postgres://{user}:{password}@{host}:{port}/{test_name}"
        ))
        .await
        .ok()?;
    Some((app, owner))
}

#[tokio::test]
async fn td_对账工具能抓出人为注入的错误() {
    let Some((app, owner)) = pools().await else {
        eprintln!("SKIP: 缺 BANK_DB_* 环境变量");
        return;
    };

    // 造一笔真实业务
    let acc = unique("TD-ACC");
    execute_command(
        &app,
        &unique("k"),
        &Command::OpenAccount(OpenAccount::new(acc.clone(), None).unwrap()),
    )
    .await
    .unwrap();
    execute_command(
        &app,
        &unique("k"),
        &Command::Deposit(Deposit::new(acc.clone(), 5_000).unwrap()),
    )
    .await
    .unwrap();

    // 1) 正常账目 → 除并行"触发器演练"可能产生的 D7 外,不应有违规
    let clean = bank_reconciler::reconcile(&app).await.unwrap();
    let unexpected: Vec<_> = clean
        .violations
        .iter()
        .filter(|v| v.check != "D7")
        .collect();
    assert!(unexpected.is_empty(), "正常账目不应有违规: {unexpected:?}");
    assert!(clean.stats.commands >= 2);

    // 2) 注入错误:属主角色把某个账户投影改错 +1 分
    let (acc_id,): (i64,) = sqlx::query_as("SELECT id FROM account WHERE account_no = $1")
        .bind(&acc)
        .fetch_one(&owner)
        .await
        .unwrap();
    sqlx::query("UPDATE account SET balance_cents = balance_cents + 1 WHERE id = $1")
        .bind(acc_id)
        .execute(&owner)
        .await
        .unwrap();

    let dirty = bank_reconciler::reconcile(&app).await.unwrap();
    assert!(!dirty.is_clean(), "被篡改的投影必须被发现");
    let hit = dirty
        .violations
        .iter()
        .any(|v| v.check == "D2" && v.detail.contains(&acc));
    assert!(hit, "D2 应报告账户 {acc}: {:?}", dirty.violations);

    // 3) 修回 → 恢复干净
    sqlx::query("UPDATE account SET balance_cents = balance_cents - 1 WHERE id = $1")
        .bind(acc_id)
        .execute(&owner)
        .await
        .unwrap();
    let fixed = bank_reconciler::reconcile(&app).await.unwrap();
    let still_bad: Vec<_> = fixed
        .violations
        .iter()
        .filter(|v| v.check != "D7")
        .collect();
    assert!(still_bad.is_empty(), "修复后应恢复一致: {still_bad:?}");
}

#[tokio::test]
async fn td_触发器被摘掉时对账必须报警() {
    let Some((app, owner)) = pools().await else {
        eprintln!("SKIP: 缺 BANK_DB_* 环境变量");
        return;
    };

    // 先确认初始是干净的(D7 通过)
    let before = bank_reconciler::reconcile(&app).await.unwrap();
    assert!(
        !before.violations.iter().any(|v| v.check == "D7"),
        "初始不应有 D7 违规: {:?}",
        before.violations
    );

    // 属主摘掉不可变触发器(模拟越权破坏),对账必须报警
    sqlx::query("ALTER TABLE ledger_entry DISABLE TRIGGER trg_ledger_entry_immutable")
        .execute(&owner)
        .await
        .unwrap();
    let during = bank_reconciler::reconcile(&app).await.unwrap();
    let flagged = during.violations.iter().any(|v| v.check == "D7");

    // 立即恢复(避免影响其他用例)
    sqlx::query("ALTER TABLE ledger_entry ENABLE TRIGGER trg_ledger_entry_immutable")
        .execute(&owner)
        .await
        .unwrap();
    let after = bank_reconciler::reconcile(&app).await.unwrap();

    assert!(
        flagged,
        "触发器被禁用时 D7 必须报警: {:?}",
        during.violations
    );
    assert!(
        !after.violations.iter().any(|v| v.check == "D7"),
        "恢复触发器后不应再有 D7: {:?}",
        after.violations
    );
}

#[tokio::test]
async fn td_应用角色确实没有分录改删权限() {
    let Some((app, _owner)) = pools().await else {
        eprintln!("SKIP: 缺 BANK_DB_* 环境变量");
        return;
    };
    let (upd, del): (bool, bool) = sqlx::query_as(
        "SELECT has_table_privilege(current_user, 'ledger_entry', 'UPDATE'),
                has_table_privilege(current_user, 'ledger_entry', 'DELETE')",
    )
    .fetch_one(&app)
    .await
    .unwrap();
    assert!(!upd && !del, "应用角色不应有分录 UPDATE/DELETE 权限");

    // 实际尝试修改(即使触发器禁用,权限也应先挡住)
    let err = sqlx::query(
        "UPDATE ledger_entry SET amount_cents = 1 WHERE id = (SELECT min(id) FROM ledger_entry)",
    )
    .execute(&app)
    .await
    .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("权限") || msg.contains("permission"),
        "应为权限不足: {msg}"
    );
}
