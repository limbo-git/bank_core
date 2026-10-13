//! bank-reconciler —— 独立对账工具(刻意不依赖 bank-store/bank-domain 的业务逻辑)。
//!
//! 目的:作为"事后仲裁者",用**独立实现**的 SQL 复核账务,而不是复用引擎自己的
//! 计算路径(否则引擎有 bug 时对账会一起错)。检查项见 PROJECT_PLAN §6 T-D。
//!
//! 检查清单:
//!   D1 每笔命令的分录借贷平衡(Σdebit = Σcredit)
//!   D2 账户投影 == Σ该账户分录(credit - debit)
//!   D3 科目投影 == Σ分录按 normal_side 折算
//!   D4 客户存款科目(2001)投影 == Σ客户账户余额
//!   D5 幂等键无重复
//!   D6 命令状态自洽(committed 必有 result;非开户命令必有分录)并报告悬挂命令
//!   D7 防线完整性:分录不可变触发器存在且启用;当前角色无 UPDATE/DELETE 权限

use serde::Serialize;
use sqlx::PgPool;

#[derive(Debug, Clone, Serialize)]
pub struct Violation {
    pub check: String,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct Stats {
    pub commands: i64,
    pub committed_commands: i64,
    pub entries: i64,
    pub accounts: i64,
    pub coa_rows: i64,
    pub dangling_commands: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct Report {
    pub checks_run: Vec<String>,
    pub violations: Vec<Violation>,
    pub stats: Stats,
}

impl Report {
    pub fn is_clean(&self) -> bool {
        self.violations.is_empty()
    }
}

/// 逐项对账。返回所有违规项(空 = 账目一致)。
pub async fn reconcile(pool: &PgPool) -> Result<Report, sqlx::Error> {
    let mut checks = Vec::new();
    let mut violations = Vec::new();

    // ---------- D1 命令级借贷平衡 ----------
    checks.push("D1 命令级借贷平衡".into());
    let unbalanced: Vec<(i64, String, i64)> = sqlx::query_as(
        "SELECT c.id, c.command_type,
                sum(CASE e.side WHEN 'debit' THEN e.amount_cents ELSE -e.amount_cents END)::bigint AS net
           FROM command_log c
           JOIN ledger_entry e ON e.command_id = c.id
          GROUP BY c.id, c.command_type
         HAVING sum(CASE e.side WHEN 'debit' THEN e.amount_cents ELSE -e.amount_cents END) <> 0",
    )
    .fetch_all(pool)
    .await?;
    for (id, ctype, net) in unbalanced {
        violations.push(Violation {
            check: "D1".into(),
            detail: format!("命令 {id}({ctype}) 借贷不平,净额 {net} 分"),
        });
    }

    // ---------- D2 账户投影 vs 分录 ----------
    checks.push("D2 账户投影与分录一致".into());
    let acc_mismatch: Vec<(String, i64, i64)> = sqlx::query_as(
        "SELECT a.account_no, a.balance_cents,
                COALESCE(sum(CASE e.side WHEN 'credit' THEN e.amount_cents ELSE -e.amount_cents END), 0)::bigint AS expected
           FROM account a
           LEFT JOIN ledger_entry e ON e.account_id = a.id
          GROUP BY a.id, a.account_no, a.balance_cents
         HAVING a.balance_cents <> COALESCE(sum(CASE e.side WHEN 'credit' THEN e.amount_cents ELSE -e.amount_cents END), 0)",
    )
    .fetch_all(pool)
    .await?;
    for (account_no, actual, expected) in acc_mismatch {
        violations.push(Violation {
            check: "D2".into(),
            detail: format!("账户 {account_no} 投影 {actual} 分,分录重算 {expected} 分"),
        });
    }

    // ---------- D3 科目投影 vs 分录 ----------
    checks.push("D3 科目投影与分录一致".into());
    let coa_mismatch: Vec<(String, i64, i64)> = sqlx::query_as(
        "SELECT c.code, cb.balance_cents,
                COALESCE(sum(CASE WHEN e.side = c.normal_side THEN e.amount_cents ELSE -e.amount_cents END), 0)::bigint AS expected
           FROM coa c
           JOIN coa_balance cb ON cb.coa_id = c.id
           LEFT JOIN ledger_entry e ON e.coa_id = c.id
          GROUP BY c.code, c.normal_side, cb.balance_cents
         HAVING cb.balance_cents <> COALESCE(sum(CASE WHEN e.side = c.normal_side THEN e.amount_cents ELSE -e.amount_cents END), 0)",
    )
    .fetch_all(pool)
    .await?;
    for (code, actual, expected) in coa_mismatch {
        violations.push(Violation {
            check: "D3".into(),
            detail: format!("科目 {code} 投影 {actual} 分,分录重算 {expected} 分"),
        });
    }

    // ---------- D4 客户存款科目 == Σ客户账户 ----------
    checks.push("D4 存款科目与客户明细一致".into());
    let row: (i64, i64) = sqlx::query_as(
        "SELECT COALESCE((SELECT cb.balance_cents FROM coa_balance cb JOIN coa c ON c.id = cb.coa_id WHERE c.code = '2001'), 0)::bigint,
                COALESCE((SELECT sum(balance_cents) FROM account), 0)::bigint",
    )
    .fetch_one(pool)
    .await?;
    if row.0 != row.1 {
        violations.push(Violation {
            check: "D4".into(),
            detail: format!("2001 科目余额 {} 分 ≠ Σ客户账户 {} 分", row.0, row.1),
        });
    }

    // ---------- D5 幂等键无重复 ----------
    checks.push("D5 幂等键唯一".into());
    let dup: Vec<(String, i64)> = sqlx::query_as(
        "SELECT idempotency_key, count(*) FROM command_log GROUP BY idempotency_key HAVING count(*) > 1",
    )
    .fetch_all(pool)
    .await?;
    for (key, n) in dup {
        violations.push(Violation {
            check: "D5".into(),
            detail: format!("幂等键 {key} 出现 {n} 次"),
        });
    }

    // ---------- D6 命令状态自洽 ----------
    checks.push("D6 命令状态自洽".into());
    let no_result: Vec<(String,)> = sqlx::query_as(
        "SELECT idempotency_key FROM command_log WHERE status = 'committed' AND result IS NULL",
    )
    .fetch_all(pool)
    .await?;
    for (key,) in no_result {
        violations.push(Violation {
            check: "D6".into(),
            detail: format!("命令 {key} 已提交但无结果快照"),
        });
    }
    let no_entries: Vec<(String, String)> = sqlx::query_as(
        "SELECT c.idempotency_key, c.command_type
           FROM command_log c
           LEFT JOIN ledger_entry e ON e.command_id = c.id
          WHERE c.status = 'committed'
            AND c.command_type NOT IN ('open_account', 'run_eod')
            AND NOT (c.command_type = 'settle_interest'
                     AND COALESCE((c.result->>'total_interest_cents')::bigint, 0) = 0)
          GROUP BY c.idempotency_key, c.command_type
         HAVING count(e.id) = 0",
    )
    .fetch_all(pool)
    .await?;
    for (key, ctype) in no_entries {
        violations.push(Violation {
            check: "D6".into(),
            detail: format!("命令 {key}({ctype}) 已提交但无分录"),
        });
    }

    // ---------- D8 日切快照试算平衡 ----------
    checks.push("D8 日切快照试算平衡".into());
    let unbalanced_eod: Vec<(String,)> =
        sqlx::query_as("SELECT day::text FROM eod_snapshot WHERE balanced = false")
            .fetch_all(pool)
            .await?;
    for (day,) in unbalanced_eod {
        violations.push(Violation {
            check: "D8".into(),
            detail: format!("{day} 日切时借贷不平"),
        });
    }

    // ---------- D7 防线完整性 ----------
    checks.push("D7 不可变防线完整性".into());
    let triggers: Vec<(String, String)> = sqlx::query_as(
        "SELECT t.tgname, t.tgenabled::text
           FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid
          WHERE c.relname = 'ledger_entry' AND NOT t.tgisinternal",
    )
    .fetch_all(pool)
    .await?;
    let enabled = triggers.iter().any(|(_, e)| e == "O" || e == "A");
    if !enabled {
        violations.push(Violation {
            check: "D7".into(),
            detail: format!("ledger_entry 不可变触发器缺失或未启用(现有: {triggers:?})"),
        });
    }
    let privs: (bool, bool) = sqlx::query_as(
        "SELECT has_table_privilege(current_user, 'ledger_entry', 'UPDATE'),
                has_table_privilege(current_user, 'ledger_entry', 'DELETE')",
    )
    .fetch_one(pool)
    .await?;
    if privs.0 || privs.1 {
        violations.push(Violation {
            check: "D7".into(),
            detail: format!(
                "当前角色 {} 拥有分录的 UPDATE={} / DELETE={} 权限(应为最小权限)",
                current_user(pool).await?,
                privs.0,
                privs.1
            ),
        });
    }

    // 统计
    let stats: (i64, i64, i64, i64, i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM command_log),
                (SELECT count(*) FROM command_log WHERE status = 'committed'),
                (SELECT count(*) FROM ledger_entry),
                (SELECT count(*) FROM account),
                (SELECT count(*) FROM coa_balance),
                (SELECT count(*) FROM command_log WHERE status IN ('received','outcome_unknown'))",
    )
    .fetch_one(pool)
    .await?;

    Ok(Report {
        checks_run: checks,
        violations,
        stats: Stats {
            commands: stats.0,
            committed_commands: stats.1,
            entries: stats.2,
            accounts: stats.3,
            coa_rows: stats.4,
            dangling_commands: stats.5,
        },
    })
}

async fn current_user(pool: &PgPool) -> Result<String, sqlx::Error> {
    let (u,): (String,) = sqlx::query_as("SELECT current_user")
        .fetch_one(pool)
        .await?;
    Ok(u)
}

/// 应用角色连接 URL(独立实现,刻意不复用引擎代码)
pub fn bank_db_url() -> Result<String, String> {
    let get =
        |k: &str| std::env::var(k).map_err(|_| format!("缺少环境变量 {k}(见 ~/.env/database.env)"));
    Ok(format!(
        "postgres://{}:{}@{}:{}/{}",
        get("BANK_DB_USER")?,
        get("BANK_DB_PASSWORD")?,
        get("BANK_DB_HOST")?,
        get("BANK_DB_PORT")?,
        get("BANK_DB_NAME")?,
    ))
}
