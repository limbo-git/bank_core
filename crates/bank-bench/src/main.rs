//! bank-bench —— 压测器(M4)。
//!
//! 场景:
//!   S1 均匀随机转账:N 个账户间随机转账
//!   S2 单账户热点:全部转账打向同一收款账户(行锁串行化最坏情形)
//!   S3 混合命令流:转账 70% + 存款 12% + 取款 3% + 冲正 10% + 查询 5%
//!
//! 对照:
//!   --mode engine:走完整命令管线(幂等领取/行锁/分录/投影)
//!   --mode raw   :同语义裸 PG(单条 CTE 语句,无分录无幂等)—— 差值 = Rust 业务层净开销
//!
//! 一次运行 = 一个 JSON 结果行(含 TPS、p50/p95/p99、失败率、对账结论),便于脚本汇总。
//!
//! 用法:
//!   bank-bench --prepare                                   # 重置压测库并建账户/充值
//!   bank-bench --mode engine --scenario s1 --concurrency 32 --ops-per-worker 200 --label s1-engine-c32

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bank_domain::{Command, Deposit, OpenAccount, Reverse, Transfer, Withdraw};
use serde_json::json;
use sqlx::PgPool;

const ACCOUNTS: usize = 1000;
const SENDERS: usize = 200;
const HOT_ACCOUNT: &str = "bench-hot";
const FUND_CENTS: i64 = 10_000_000; // 每个账户 ¥100,000
const TRANSFER_CENTS: i64 = 100; // ¥1
const DEPOSIT_CENTS: i64 = 1_000; // ¥10
const WITHDRAW_CENTS: i64 = 100; // ¥1

fn arg(name: &str) -> Option<String> {
    let args: Vec<String> = std::env::args().collect();
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1).cloned())
}
fn flag(name: &str) -> bool {
    std::env::args().any(|a| a == name)
}

fn acc_no(i: usize) -> String {
    format!("bench-{i:04}")
}

/// 独立实现的账户级连接 URL(与对账工具一致,不复用引擎代码)
fn bench_db_name() -> String {
    let base = std::env::var("BANK_DB_NAME").expect("缺少 BANK_DB_NAME");
    // 裸 PG 模式不维护账本(无分录),必须用独立库,否则会污染引擎库的对账基线
    if std::env::args().any(|a| a == "raw") || arg("--mode").as_deref() == Some("raw") {
        format!("{base}_bench_raw")
    } else {
        format!("{base}_bench")
    }
}
fn app_url() -> String {
    format!(
        "postgres://{}:{}@{}:{}/{}",
        std::env::var("BANK_DB_USER").unwrap(),
        std::env::var("BANK_DB_PASSWORD").unwrap(),
        std::env::var("BANK_DB_HOST").unwrap(),
        std::env::var("BANK_DB_PORT").unwrap(),
        bench_db_name()
    )
}
fn owner_url() -> String {
    format!(
        "postgres://{}:{}@{}:{}/{}",
        std::env::var("BANK_DB_OWNER_USER").unwrap(),
        std::env::var("BANK_DB_OWNER_PASSWORD").unwrap(),
        std::env::var("BANK_DB_HOST").unwrap(),
        std::env::var("BANK_DB_PORT").unwrap(),
        bench_db_name()
    )
}

async fn app_pool(sync_off: bool, concurrency: usize) -> (PgPool, u32) {
    // 连接池受 PG max_connections 约束:池大小 = min(并发+4, 服务端上限-8)
    // 128/512 档在本机(上限 100)即为"池受限"档 —— 多余 worker 会在池上排队,
    // 如实记录 pool_max 便于解读。要真按 512 连接压测需提高 max_connections 并重启 PG。
    let probe = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&app_url())
        .await
        .expect("连接压测库失败");
    let (pg_max,): (i32,) =
        sqlx::query_as("SELECT setting::int FROM pg_settings WHERE name='max_connections'")
            .fetch_one(&probe)
            .await
            .expect("读取 max_connections 失败");
    probe.close().await;
    let pool_max = ((concurrency as u32 + 4).min((pg_max as u32).saturating_sub(8))).max(2);

    let mut opts = sqlx::postgres::PgPoolOptions::new().max_connections(pool_max);
    if sync_off {
        opts = opts.after_connect(|conn, _| {
            Box::pin(async move {
                sqlx::query("SET synchronous_commit = off")
                    .execute(conn)
                    .await?;
                Ok(())
            })
        });
    }
    let pool = opts.connect(&app_url()).await.expect("连接压测库失败");
    (pool, pool_max)
}

/// 简易 xorshift 随机数(避免引入 rand 依赖)
struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed | 1)
    }
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

// ---------- 准备数据 ----------

async fn prepare() {
    let owner = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .connect(&owner_url())
        .await
        .expect("连接属主失败");
    bank_store::migrate(&owner).await.expect("迁移失败");
    sqlx::query(
        "TRUNCATE ledger_entry, command_log, account_daily_balance, eod_snapshot,
                  interest_settlement, account, coa_balance RESTART IDENTITY CASCADE",
    )
    .execute(&owner)
    .await
    .expect("清空压测库失败");
    sqlx::query("INSERT INTO coa_balance (coa_id, balance_cents) SELECT id, 0 FROM coa")
        .execute(&owner)
        .await
        .expect("重播科目余额失败");
    owner.close().await;

    // 业务连接:必须用最小权限的应用角色(压测走真实权限路径)
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(16)
        .connect(&app_url())
        .await
        .expect("连接压测库失败");

    let t0 = Instant::now();
    for i in 0..ACCOUNTS {
        let no = acc_no(i);
        bank_store::commands::execute_command(
            &pool,
            &format!("prep-open-{i}"),
            &Command::OpenAccount(OpenAccount::new(no.clone(), None).unwrap()),
        )
        .await
        .expect("开户失败");
        bank_store::commands::execute_command(
            &pool,
            &format!("prep-dep-{i}"),
            &Command::Deposit(Deposit::new(no, FUND_CENTS).unwrap()),
        )
        .await
        .expect("充值失败");
    }
    bank_store::commands::execute_command(
        &pool,
        "prep-open-hot",
        &Command::OpenAccount(OpenAccount::new(HOT_ACCOUNT.to_string(), None).unwrap()),
    )
    .await
    .expect("热点账户开户失败");

    println!(
        "{}",
        json!({
            "event": "prepared",
            "accounts": ACCOUNTS,
            "funded_cents_each": FUND_CENTS,
            "hot_account": HOT_ACCOUNT,
            "elapsed_ms": t0.elapsed().as_millis(),
        })
    );
}

// ---------- 压测执行 ----------

struct RunResult {
    ok: AtomicU64,
    business_rejected: AtomicU64,
    errors: AtomicU64,
    /// 错误样本(最多留 5 条,便于定位问题类别)
    error_samples: Mutex<Vec<String>>,
}

fn record_error(stats: &RunResult, err: String) {
    stats.errors.fetch_add(1, Ordering::Relaxed);
    let mut s = stats.error_samples.lock().unwrap();
    if s.len() < 5 && !s.iter().any(|x| x == &err) {
        s.push(err);
    }
}

fn scenario_pick(scenario: &str, rng: &mut Rng) -> (usize, usize) {
    match scenario {
        // S2:收款方固定为热点账户(用 SENDERS 内的编号做付款方,收款走 HOT)
        "s2" => (rng.below(SENDERS as u64) as usize, usize::MAX),
        _ => {
            let a = rng.below(ACCOUNTS as u64) as usize;
            let mut b = rng.below(ACCOUNTS as u64) as usize;
            if b == a {
                b = (b + 1) % ACCOUNTS;
            }
            (a, b)
        }
    }
}

async fn run(
    mode: &str,
    scenario: &str,
    concurrency: usize,
    ops_per_worker: usize,
    sync_off: bool,
    label: &str,
) {
    let (pool, pool_max) = app_pool(sync_off, concurrency).await;
    let stats = Arc::new(RunResult {
        ok: AtomicU64::new(0),
        business_rejected: AtomicU64::new(0),
        errors: AtomicU64::new(0),
        error_samples: Mutex::new(Vec::new()),
    });
    // S3:冲正目标 = 本轮的存款命令键(边生产边消费,天然不会重复冲正)
    let reverse_targets: Arc<Mutex<VecDeque<String>>> = Arc::new(Mutex::new(VecDeque::new()));

    let started = Instant::now();
    let mut handles = Vec::with_capacity(concurrency);
    for w in 0..concurrency {
        let pool = pool.clone();
        let stats = stats.clone();
        let targets = reverse_targets.clone();
        let scenario = scenario.to_string();
        let mode = mode.to_string();
        handles.push(tokio::spawn(async move {
            let mut rng = Rng::new(0x9E3779B97F4A7C15 ^ ((w as u64 + 1) * 0x100000001B3));
            let mut latencies: Vec<u64> = Vec::with_capacity(ops_per_worker);
            for i in 0..ops_per_worker {
                let op_start = Instant::now();
                let (a, b) = scenario_pick(&scenario, &mut rng);
                let key = format!("bench-{scenario}-{w}-{i}");
                // 决定本轮命令类型
                let roll = if scenario == "s3" { rng.below(100) } else { 0 };
                let outcome: Result<bool, String> = match (mode.as_str(), roll) {
                    // ---- 裸 PG 对照:单条 CTE 语句 ----
                    ("raw", _) => {
                        let to = if b == usize::MAX {
                            HOT_ACCOUNT.to_string()
                        } else {
                            acc_no(b)
                        };
                        let res = sqlx::query(
                            "WITH d AS (UPDATE account SET balance_cents = balance_cents - $1
                                         WHERE account_no = $2 AND balance_cents >= $1 RETURNING id)
                             UPDATE account SET balance_cents = balance_cents + $1
                              WHERE account_no = $3 AND EXISTS (SELECT 1 FROM d)",
                        )
                        .bind(TRANSFER_CENTS)
                        .bind(acc_no(a))
                        .bind(to)
                        .execute(&pool)
                        .await;
                        match res {
                            Ok(r) => Ok(r.rows_affected() == 1),
                            Err(e) => Err(e.to_string()),
                        }
                    }
                    // ---- 引擎路径 ----
                    ("engine", r) if r < 70 => {
                        let to = if b == usize::MAX {
                            HOT_ACCOUNT.to_string()
                        } else {
                            acc_no(b)
                        };
                        let cmd = Command::Transfer(
                            Transfer::new(acc_no(a), to, TRANSFER_CENTS).unwrap(),
                        );
                        match bank_store::commands::execute_command(&pool, &key, &cmd).await {
                            Ok(_) => Ok(true),
                            Err(bank_store::StoreError::InsufficientFunds(_)) => Ok(false),
                            Err(e) => Err(e.to_string()),
                        }
                    }
                    ("engine", r) if r < 82 => {
                        let cmd = Command::Deposit(Deposit::new(acc_no(a), DEPOSIT_CENTS).unwrap());
                        match bank_store::commands::execute_command(&pool, &key, &cmd).await {
                            Ok(_) => {
                                targets.lock().unwrap().push_back(key.clone());
                                Ok(true)
                            }
                            Err(e) => Err(e.to_string()),
                        }
                    }
                    ("engine", r) if r < 85 => {
                        let cmd =
                            Command::Withdraw(Withdraw::new(acc_no(a), WITHDRAW_CENTS).unwrap());
                        match bank_store::commands::execute_command(&pool, &key, &cmd).await {
                            Ok(_) => Ok(true),
                            Err(bank_store::StoreError::InsufficientFunds(_)) => Ok(false),
                            Err(e) => Err(e.to_string()),
                        }
                    }
                    ("engine", r) if r < 95 => {
                        let target = targets.lock().unwrap().pop_front();
                        match target {
                            Some(t) => {
                                let cmd = Command::Reverse(Reverse::new(t).unwrap());
                                match bank_store::commands::execute_command(&pool, &key, &cmd).await
                                {
                                    Ok(_) => Ok(true),
                                    Err(bank_store::StoreError::AlreadyReversed(_))
                                    | Err(bank_store::StoreError::WouldOverdraw(_))
                                    | Err(bank_store::StoreError::NotReversible(_)) => Ok(false),
                                    Err(e) => Err(e.to_string()),
                                }
                            }
                            None => Ok(false), // 暂无冲正目标,计为业务未执行
                        }
                    }
                    _ => {
                        // 5% 查询:只读余额
                        let res = sqlx::query_scalar::<_, i64>(
                            "SELECT balance_cents FROM account WHERE account_no = $1",
                        )
                        .bind(acc_no(a))
                        .fetch_optional(&pool)
                        .await;
                        match res {
                            Ok(Some(_)) => Ok(true),
                            Ok(None) => Ok(false),
                            Err(e) => Err(e.to_string()),
                        }
                    }
                };
                latencies.push(op_start.elapsed().as_nanos() as u64);
                match outcome {
                    Ok(true) => {
                        stats.ok.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(false) => {
                        stats.business_rejected.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(msg) => {
                        record_error(&stats, msg);
                    }
                }
            }
            latencies
        }));
    }

    let mut all: Vec<u64> = Vec::new();
    for h in handles {
        all.extend(h.await.expect("worker 崩溃"));
    }
    let elapsed = started.elapsed();
    all.sort_unstable();
    let pct = |p: f64| -> f64 {
        if all.is_empty() {
            return 0.0;
        }
        let idx = ((all.len() as f64 - 1.0) * p).round() as usize;
        all[idx] as f64 / 1_000_000.0
    };
    let ops = (all.len() as u64).max(1);
    let ok = stats.ok.load(Ordering::Relaxed);
    let rej = stats.business_rejected.load(Ordering::Relaxed);
    let err = stats.errors.load(Ordering::Relaxed);
    let tps = ok as f64 / elapsed.as_secs_f64();
    let total_tps = ops as f64 / elapsed.as_secs_f64();

    // 每轮压测后强制对账(裸 PG 模式不维护账本,不参与对账)
    let (clean, violations) = if mode == "raw" {
        (true, 0)
    } else {
        let report = bank_reconciler::reconcile(&pool).await.expect("对账失败");
        (report.is_clean(), report.violations.len())
    };

    println!(
        "{}",
        json!({
            "label": label,
            "mode": mode,
            "scenario": scenario,
            "concurrency": concurrency,
            "pool_max": pool_max,
            "ops": ops,
            "ok": ok,
            "business_rejected": rej,
            "errors": err,
            "elapsed_ms": (elapsed.as_millis() as u64),
            "tps_ok": (tps * 10.0).round() / 10.0,
            "tps_all": (total_tps * 10.0).round() / 10.0,
            "p50_ms": (pct(0.50) * 100.0).round() / 100.0,
            "p95_ms": (pct(0.95) * 100.0).round() / 100.0,
            "p99_ms": (pct(0.99) * 100.0).round() / 100.0,
            "max_ms": (all.last().copied().unwrap_or(0) as f64 / 1_000_000.0 * 100.0).round() / 100.0,
            "sync_commit": if sync_off { "off" } else { "on" },
            "reconciled_clean": clean,
            "reconcile_violations": violations,
            "error_samples": stats.error_samples.lock().unwrap().clone(),
        })
    );

    tokio::time::sleep(Duration::from_millis(200)).await; // 让连接池优雅回收
}

#[tokio::main]
async fn main() {
    if flag("--prepare") {
        prepare().await;
        return;
    }
    let mode = arg("--mode").unwrap_or_else(|| "engine".into());
    let scenario = arg("--scenario").unwrap_or_else(|| "s1".into());
    let concurrency: usize = arg("--concurrency")
        .and_then(|v| v.parse().ok())
        .unwrap_or(32);
    let ops_per_worker: usize = arg("--ops-per-worker")
        .and_then(|v| v.parse().ok())
        .unwrap_or(200);
    let label = arg("--label").unwrap_or_else(|| format!("{scenario}-{mode}-c{concurrency}"));
    let sync_off = arg("--sync-commit").map(|v| v == "off").unwrap_or(false);
    run(
        &mode,
        &scenario,
        concurrency,
        ops_per_worker,
        sync_off,
        &label,
    )
    .await;
}
