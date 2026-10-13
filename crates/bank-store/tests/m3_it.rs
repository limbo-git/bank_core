//! T-E 分布式预演测试(在**隔离库** `*_replay` 上跑,可反复重放)。
//!
//! 覆盖:M3 冲正 / 区间计息 / 日切规则、确定性重放(T-E1)、时钟注入(T-E3)、
//! kill -9 无半笔与重试收敛(T-E2)。
//! 库内测试用一把互斥锁串行化(它们共享同一库且做 TRUNCATE)。

use std::sync::atomic::{AtomicU64, Ordering};

use bank_domain::{
    Command, Deposit, OpenAccount, Reverse, RunEod, SettleInterest, Transfer, Withdraw,
};
use bank_reconciler::reconcile;
use bank_store::commands::execute_command;
use bank_store::queries;
use bank_store::StoreError;
use sqlx::PgPool;
use tokio::sync::Mutex;

/// 隔离库上的测试互斥执行
static SERIAL: Mutex<()> = Mutex::const_new(());

static SEQ: AtomicU64 = AtomicU64::new(0);

fn unique(tag: &str) -> String {
    let n = SEQ.fetch_add(1, Ordering::SeqCst);
    format!("{tag}-{n}-{}", std::process::id())
}

/// 重放库连接:(应用池, 属主池)
async fn replay_pools() -> Option<(PgPool, PgPool)> {
    let host = std::env::var("BANK_DB_HOST").ok()?;
    let port = std::env::var("BANK_DB_PORT").ok()?;
    let name = std::env::var("BANK_DB_NAME").ok()?;
    let user = std::env::var("BANK_DB_USER").ok()?;
    let password = std::env::var("BANK_DB_PASSWORD").ok()?;
    let owner_user = std::env::var("BANK_DB_OWNER_USER").ok()?;
    let owner_password = std::env::var("BANK_DB_OWNER_PASSWORD").ok()?;
    let replay_name = replay_db_name(&name);

    let owner = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .connect(&format!(
            "postgres://{owner_user}:{owner_password}@{host}:{port}/{replay_name}"
        ))
        .await
        .ok()?;
    bank_store::migrate(&owner).await.ok()?;

    let app = sqlx::postgres::PgPoolOptions::new()
        .max_connections(8)
        .connect(&format!(
            "postgres://{user}:{password}@{host}:{port}/{replay_name}"
        ))
        .await
        .ok()?;
    Some((app, owner))
}

fn replay_db_name(base: &str) -> String {
    format!("{base}_replay")
}

/// 清空重放库(属主操作;TRUNCATE 不受"分录不可改"触发器影响),并重新播种科目余额行
async fn reset(owner: &PgPool) {
    sqlx::query(
        "TRUNCATE ledger_entry, command_log, account_daily_balance, eod_snapshot,
                  interest_settlement, account, coa_balance RESTART IDENTITY CASCADE",
    )
    .execute(owner)
    .await
    .expect("清空重放库失败");
    sqlx::query("INSERT INTO coa_balance (coa_id, balance_cents) SELECT id, 0 FROM coa")
        .execute(owner)
        .await
        .expect("重新播种科目余额失败");
}

async fn open(app: &PgPool, no: &str) {
    execute_command(
        app,
        &unique("k-open"),
        &Command::OpenAccount(OpenAccount::new(no.to_string(), None).unwrap()),
    )
    .await
    .unwrap();
}

async fn deposit(app: &PgPool, no: &str, cents: i64) -> String {
    let key = unique("k-dep");
    execute_command(
        app,
        &key,
        &Command::Deposit(Deposit::new(no.to_string(), cents).unwrap()),
    )
    .await
    .unwrap();
    key
}

/// 状态指纹:只取业务量(账户/分录/科目/日切/计息/命令),刻意排除自增 id 与时间戳
async fn fingerprint(app: &PgPool) -> String {
    let mut out = String::new();
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT 'account', account_no || ':' || balance_cents || ':' || status FROM account
         UNION ALL
         SELECT 'coa_balance', c.code || ':' || cb.balance_cents
           FROM coa_balance cb JOIN coa c ON c.id = cb.coa_id
         UNION ALL
         SELECT 'entry', c.command_type || ':' || e.seq || ':' || e.side || ':' || e.amount_cents
                          || ':' || COALESCE(a.account_no, '-') || ':' || co.code
                          || ':' || COALESCE(rev.idempotency_key, '-')
           FROM ledger_entry e
           JOIN command_log c ON c.id = e.command_id
           JOIN coa co ON co.id = e.coa_id
           LEFT JOIN account a ON a.id = e.account_id
           LEFT JOIN command_log rev ON rev.id = e.reverses_command_id
         UNION ALL
         SELECT 'eod', day::text || ':' || balanced FROM eod_snapshot
         UNION ALL
         SELECT 'daily', a.account_no || ':' || d.day::text || ':' || d.balance_cents
           FROM account_daily_balance d JOIN account a ON a.id = d.account_id
         UNION ALL
         SELECT 'interest', period_start::text || ':' || period_end::text || ':' || annual_rate_bp
           FROM interest_settlement
         UNION ALL
         SELECT 'command', idempotency_key || ':' || command_type || ':' || status FROM command_log
         ORDER BY 1, 2",
    )
    .fetch_all(app)
    .await
    .unwrap();
    for (k, v) in rows {
        out.push_str(&format!("{k}|{v}\n"));
    }
    out
}

/// 固定的"业务剧本":开户 → 存取 → 转账 → 冲正 → 日切×2 → 区间计息
async fn run_script() -> Vec<(String, bank_domain::Command)> {
    let a = "REPLAY-A";
    let b = "REPLAY-B";
    let c = "REPLAY-C";
    let mut steps: Vec<(String, bank_domain::Command)> = Vec::new();
    let mut push = |key: String, cmd: bank_domain::Command| steps.push((key, cmd));

    for no in [a, b, c] {
        let key = format!("script-open-{no}");
        push(
            key,
            Command::OpenAccount(OpenAccount::new(no.to_string(), None).unwrap()),
        );
    }
    let k = "script-dep-a".to_string();
    push(
        k,
        Command::Deposit(Deposit::new(a.to_string(), 100_000).unwrap()),
    );
    let k = "script-dep-b".to_string();
    push(
        k,
        Command::Deposit(Deposit::new(b.to_string(), 50_000).unwrap()),
    );
    let k = "script-wd-a".to_string();
    push(
        k,
        Command::Withdraw(Withdraw::new(a.to_string(), 10_000).unwrap()),
    );
    let k = "script-trf-a-b".to_string();
    push(
        k,
        Command::Transfer(Transfer::new(a.to_string(), b.to_string(), 5_000).unwrap()),
    );
    let k = "script-rev-dep-b".to_string();
    push(k, Command::Reverse(Reverse::new("script-dep-b").unwrap()));
    let k = "script-eod-1".to_string();
    push(k, Command::RunEod(RunEod::new("2026-01-01").unwrap()));
    let k = "script-eod-2".to_string();
    push(k, Command::RunEod(RunEod::new("2026-01-02").unwrap()));
    let k = "script-interest".to_string();
    push(
        k,
        Command::SettleInterest(SettleInterest::new("2026-01-01", "2026-01-03", 360).unwrap()),
    );
    steps
}

// ---------- M3-1 冲正规则与账务 ----------

#[tokio::test]
async fn m3_冲正_规则与账务() {
    let _guard = SERIAL.lock().await;
    let Some((app, owner)) = replay_pools().await else {
        eprintln!("SKIP: 缺 BANK_DB_* 环境变量");
        return;
    };
    reset(&owner).await;

    open(&app, "REV-A").await;
    let dep_key = deposit(&app, "REV-A", 10_000).await;
    let wd_key = unique("k-wd");
    execute_command(
        &app,
        &wd_key,
        &Command::Withdraw(Withdraw::new("REV-A".to_string(), 3_000).unwrap()),
    )
    .await
    .unwrap();

    // 冲正"存款"会让账户变负(钱已取走 3000)→ 必须拒绝
    let err = execute_command(
        &app,
        &unique("k-rev"),
        &Command::Reverse(Reverse::new(dep_key.clone()).unwrap()),
    )
    .await
    .unwrap_err();
    assert!(
        matches!(err, StoreError::WouldOverdraw(_)),
        "应拒绝会透支的冲正,实际: {err:?}"
    );

    // 冲正那笔取款 → 余额回到 10000
    execute_command(
        &app,
        &unique("k-rev"),
        &Command::Reverse(Reverse::new(wd_key.clone()).unwrap()),
    )
    .await
    .unwrap();
    let acc = queries::get_account(&app, "REV-A").await.unwrap().unwrap();
    assert_eq!(acc.balance_cents, 10_000, "冲正取款后余额应恢复");

    // 同一笔原交易再次冲正 → 拒绝
    let err = execute_command(
        &app,
        &unique("k-rev"),
        &Command::Reverse(Reverse::new(wd_key.clone()).unwrap()),
    )
    .await
    .unwrap_err();
    assert!(matches!(err, StoreError::AlreadyReversed(_)), "{err:?}");

    // 冲正开户命令 / 不存在的命令 / 冲正命令
    let err = execute_command(
        &app,
        &unique("k-rev"),
        &Command::Reverse(Reverse::new("script-open-x".to_string()).unwrap()),
    )
    .await
    .unwrap_err();
    assert!(matches!(err, StoreError::OriginalNotFound(_)), "{err:?}");

    // 原分录一条都不能少,冲正分录必须引用原命令
    let (orig_entries,): (i64,) = sqlx::query_as(
        "SELECT count(*) FROM ledger_entry e JOIN command_log c ON c.id = e.command_id
          WHERE c.idempotency_key = $1",
    )
    .bind(&wd_key)
    .fetch_one(&app)
    .await
    .unwrap();
    assert_eq!(orig_entries, 2, "原取款两笔分录必须原样保留");
    let (rev_entries,): (i64,) =
        sqlx::query_as("SELECT count(*) FROM ledger_entry WHERE reverses_command_id IS NOT NULL")
            .fetch_one(&app)
            .await
            .unwrap();
    assert_eq!(rev_entries, 2, "冲正应追加两笔反向分录");

    let report = reconcile(&app).await.unwrap();
    assert!(
        report.is_clean(),
        "冲正后账目必须一致: {:?}",
        report.violations
    );
}

// ---------- M3-2 日切与区间计息(含时钟注入) ----------

#[tokio::test]
async fn m3_日切与区间计息() {
    let _guard = SERIAL.lock().await;
    let Some((app, owner)) = replay_pools().await else {
        eprintln!("SKIP: 缺 BANK_DB_* 环境变量");
        return;
    };
    reset(&owner).await;

    open(&app, "INT-A").await;
    deposit(&app, "INT-A", 100_000).await; // 1000.00 元

    // 日切 2026-01-01
    let out = execute_command(
        &app,
        &unique("k-eod"),
        &Command::RunEod(RunEod::new("2026-01-01").unwrap()),
    )
    .await
    .unwrap();
    assert_eq!(out.result["balanced"], true);
    assert_eq!(out.result["accounts_snapshotted"], 1);

    // 同日重复日切 → 拒绝
    let err = execute_command(
        &app,
        &unique("k-eod"),
        &Command::RunEod(RunEod::new("2026-01-01").unwrap()),
    )
    .await
    .unwrap_err();
    assert!(matches!(err, StoreError::DayAlreadyClosed(_)), "{err:?}");

    execute_command(
        &app,
        &unique("k-eod"),
        &Command::RunEod(RunEod::new("2026-01-02").unwrap()),
    )
    .await
    .unwrap();

    // 区间 [01-01, 01-03) 共 2 天,日积数 = 2 × 100000;年利率 360bp(3.60%)
    // 独立计算期望值:round(2*100000*360 / (10000*360)) = 20 分
    let rate_bp = 360_i64;
    let days = 2_i128;
    let expect_interest =
        i64::try_from((days * 100_000 * rate_bp as i128 + (10_000 * 360) / 2) / (10_000 * 360))
            .unwrap();
    assert_eq!(expect_interest, 20, "测试自己的期望算法应先自洽");

    let out = execute_command(
        &app,
        &unique("k-int"),
        &Command::SettleInterest(SettleInterest::new("2026-01-01", "2026-01-03", rate_bp).unwrap()),
    )
    .await
    .unwrap();
    assert_eq!(
        out.result["total_interest_cents"], expect_interest,
        "引擎计息必须与独立计算一致"
    );
    assert_eq!(out.result["accounts_credited"], 1);

    // 余额增加利息
    let acc = queries::get_account(&app, "INT-A").await.unwrap().unwrap();
    assert_eq!(acc.balance_cents, 100_000 + expect_interest);

    // 同区间重复结算 → 拒绝
    let err = execute_command(
        &app,
        &unique("k-int"),
        &Command::SettleInterest(SettleInterest::new("2026-01-01", "2026-01-03", rate_bp).unwrap()),
    )
    .await
    .unwrap_err();
    assert!(
        matches!(err, StoreError::PeriodAlreadySettled(_)),
        "{err:?}"
    );

    // 利息分录:一笔利息费用(借)+ 一笔客户存款(贷)
    let entries: Vec<(String, i64)> = sqlx::query_as(
        "SELECT e.side, e.amount_cents FROM ledger_entry e JOIN command_log c ON c.id = e.command_id
          WHERE c.command_type = 'settle_interest' ORDER BY e.seq",
    )
    .fetch_all(&app)
    .await
    .unwrap();
    assert_eq!(
        entries,
        vec![
            ("debit".to_string(), expect_interest),
            ("credit".to_string(), expect_interest)
        ]
    );

    let report = reconcile(&app).await.unwrap();
    assert!(
        report.is_clean(),
        "计息后账目必须一致: {:?}",
        report.violations
    );
}

// ---------- M3-3 确定性重放(同一命令流重放两遍,状态必须一致) ----------

#[tokio::test]
async fn m3_确定性重放_两遍状态一致() {
    let _guard = SERIAL.lock().await;
    let Some((app, owner)) = replay_pools().await else {
        eprintln!("SKIP: 缺 BANK_DB_* 环境变量");
        return;
    };

    // 第一遍
    reset(&owner).await;
    for (key, cmd) in run_script().await {
        execute_command(&app, &key, &cmd)
            .await
            .unwrap_or_else(|e| panic!("剧本执行失败 {key}: {e:?}"));
    }
    let fp1 = fingerprint(&app).await;
    let report1 = reconcile(&app).await.unwrap();
    assert!(
        report1.is_clean(),
        "剧本跑完必须账目一致: {:?}",
        report1.violations
    );

    // 第二遍:清空后原样重放(同幂等键、同命令)
    reset(&owner).await;
    for (key, cmd) in run_script().await {
        execute_command(&app, &key, &cmd).await.unwrap();
    }
    let fp2 = fingerprint(&app).await;

    assert_eq!(
        fp1, fp2,
        "同一命令流重放两遍,业务状态必须逐字节一致(分布式重放的前提)"
    );
    assert!(!fp1.is_empty());
}

// ---------- M3-4 kill -9:无半笔 + 重试收敛 ----------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_kill9_无半笔且重试收敛() {
    let _guard = SERIAL.lock().await;
    let Some((app, owner)) = replay_pools().await else {
        eprintln!("SKIP: 缺 BANK_DB_* 环境变量");
        return;
    };
    reset(&owner).await;

    open(&app, "CRASH-A").await;
    open(&app, "CRASH-B").await;
    deposit(&app, "CRASH-A", 1_000_000).await;
    deposit(&app, "CRASH-B", 1_000_000).await;

    // 启动跑批进程,持续转账,然后 SIGKILL
    let bin = env!("CARGO_BIN_EXE_crash_runner");
    let base_name = std::env::var("BANK_DB_NAME").unwrap();
    let mut child = std::process::Command::new(bin)
        .args([
            "--from",
            "CRASH-A",
            "--to",
            "CRASH-B",
            "--amount-cents",
            "10",
        ])
        .env("BANK_DB_NAME", replay_db_name(&base_name))
        .stdout(std::process::Stdio::piped())
        .spawn()
        .expect("启动 crash_runner 失败");
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
    child.kill().expect("SIGKILL 失败"); // kill() 在 Unix 上即 SIGKILL
    let _ = child.wait();

    // 1) 无半笔:任何已提交命令都必须完整(有分录、借贷平衡、投影一致)
    let report = reconcile(&app).await.unwrap();
    assert!(
        report.is_clean(),
        "kill -9 后账目必须一致(不得有半笔): {:?}",
        report.violations
    );
    assert!(report.stats.committed_commands > 0, "跑批应已提交若干命令");

    // 2) 不存在"已领取但未完成"的悬挂命令(事务未提交即回滚,不留痕)
    let (dangling,): (i64,) = sqlx::query_as(
        "SELECT count(*) FROM command_log WHERE status IN ('received','outcome_unknown')",
    )
    .fetch_one(&app)
    .await
    .unwrap();
    assert_eq!(dangling, 0, "崩溃不应留下悬挂命令");

    // 3) 重试收敛:取最后一条已提交命令,用同键同 payload 重放 → 必须返回原结果
    let row: (String, String, Option<serde_json::Value>) = sqlx::query_as(
        "SELECT idempotency_key, command_type, result FROM command_log
          WHERE command_type = 'transfer' ORDER BY id DESC LIMIT 1",
    )
    .fetch_one(&app)
    .await
    .unwrap();
    let (key, ctype, stored_result) = row;
    assert_eq!(ctype, "transfer");
    let payload: (String, String, i64) = sqlx::query_as(
        "SELECT payload->>'from_account_no', payload->>'to_account_no',
                (payload->>'amount_cents')::bigint
           FROM command_log WHERE idempotency_key = $1",
    )
    .bind(&key)
    .fetch_one(&app)
    .await
    .unwrap();
    let out = execute_command(
        &app,
        &key,
        &Command::Transfer(Transfer::new(payload.0, payload.1, payload.2).unwrap()),
    )
    .await
    .unwrap();
    assert!(out.replay, "同键重试必须识别为重放,不得重复记账");
    assert_eq!(
        out.result,
        stored_result.unwrap_or(serde_json::json!({})),
        "重放必须返回崩溃前落库的原结果"
    );

    // 4) 重试后账目仍一致
    let report2 = reconcile(&app).await.unwrap();
    assert!(report2.is_clean(), "{:?}", report2.violations);
}
