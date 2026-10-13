//! T-C 并发正确性测试(真实 PG + 最小权限角色)。
//!
//! 每个风暴结束后都跑一次独立对账工具(bank-reconciler)复核账目。
//! 用例清单见 PROJECT_PLAN §6 T-C。

use std::sync::atomic::{AtomicU64, Ordering};

use bank_domain::{Command, Deposit, OpenAccount, Transfer, Withdraw};
use bank_store::commands::execute_command;
use bank_store::queries;
use bank_store::StoreError;
use sqlx::PgPool;

static SEQ: AtomicU64 = AtomicU64::new(0);

fn unique(tag: &str) -> String {
    let n = SEQ.fetch_add(1, Ordering::SeqCst);
    format!("{tag}-{n}-{}", std::process::id())
}

async fn test_pool() -> Option<PgPool> {
    let host = std::env::var("BANK_DB_HOST").ok()?;
    let port = std::env::var("BANK_DB_PORT").ok()?;
    let name = std::env::var("BANK_DB_NAME").ok()?;
    let user = std::env::var("BANK_DB_USER").ok()?;
    let password = std::env::var("BANK_DB_PASSWORD").ok()?;
    let owner_user = std::env::var("BANK_DB_OWNER_USER").ok()?;
    let owner_password = std::env::var("BANK_DB_OWNER_PASSWORD").ok()?;
    let test_name = format!("{name}_test");

    let owner_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&format!(
            "postgres://{owner_user}:{owner_password}@{host}:{port}/{test_name}"
        ))
        .await
        .ok()?;
    bank_store::migrate(&owner_pool).await.ok()?;
    owner_pool.close().await;

    sqlx::postgres::PgPoolOptions::new()
        .max_connections(32)
        .connect(&format!(
            "postgres://{user}:{password}@{host}:{port}/{test_name}"
        ))
        .await
        .ok()
}

async fn open(pool: &PgPool, tag: &str) -> String {
    let no = unique(tag);
    let cmd = Command::OpenAccount(OpenAccount::new(no.clone(), None).unwrap());
    execute_command(pool, &unique("k-open"), &cmd)
        .await
        .unwrap();
    no
}

async fn deposit(pool: &PgPool, account_no: &str, cents: i64) {
    execute_command(
        pool,
        &unique("k-dep"),
        &Command::Deposit(Deposit::new(account_no, cents).unwrap()),
    )
    .await
    .unwrap();
}

/// 风暴结束后必须账目一致(独立对账工具复核)
async fn assert_reconciled(pool: &PgPool, 场景: &str) {
    let report = bank_reconciler::reconcile(pool).await.unwrap();
    assert!(
        report.is_clean(),
        "{场景}: 对账发现违规 {:?}",
        report.violations
    );
}

// ---------- T-C1 同账户并发扣款:恰好成功到余额为 0,绝不超扣 ----------

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn tc1_同账户并发转账_不超扣() {
    let Some(pool) = test_pool().await else {
        eprintln!("SKIP: 缺 BANK_DB_* 环境变量");
        return;
    };
    let payer = open(&pool, "TC1-PAYER").await;
    deposit(&pool, &payer, 10_000).await; // 100 次 × 100 分

    let mut receivers = Vec::new();
    for _ in 0..20 {
        receivers.push(open(&pool, "TC1-RECV").await);
    }

    let mut join = tokio::task::JoinSet::new();
    for i in 0..200 {
        let pool = pool.clone();
        let from = payer.clone();
        let to = receivers[i % receivers.len()].clone();
        join.spawn(async move {
            execute_command(
                &pool,
                &unique("k-tc1"),
                &Command::Transfer(Transfer::new(from, to, 100).unwrap()),
            )
            .await
        });
    }
    let mut ok = 0;
    let mut insufficient = 0;
    while let Some(r) = join.join_next().await {
        match r.unwrap() {
            Ok(_) => ok += 1,
            Err(StoreError::InsufficientFunds(_)) => insufficient += 1,
            Err(e) => panic!("意外错误: {e:?}"),
        }
    }
    assert_eq!(ok, 100, "余额只够 100 笔成功");
    assert_eq!(insufficient, 100, "其余必须安全失败");

    let p = queries::get_account(&pool, &payer).await.unwrap().unwrap();
    assert_eq!(p.balance_cents, 0, "付款账户必须恰好清零,绝不超扣");
    assert!(p.balance_cents >= 0);
    assert_reconciled(&pool, "T-C1").await;
}

// ---------- T-C2 双账户互转:无死锁,总量守恒 ----------

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn tc2_双账户互转并发_无死锁() {
    let Some(pool) = test_pool().await else {
        eprintln!("SKIP: 缺 BANK_DB_* 环境变量");
        return;
    };
    let a = open(&pool, "TC2-A").await;
    let b = open(&pool, "TC2-B").await;
    deposit(&pool, &a, 100_000).await;
    deposit(&pool, &b, 100_000).await;

    let mut join = tokio::task::JoinSet::new();
    for _ in 0..100 {
        // 每个回合两个方向同时发起:考验加锁顺序(按 account_no 排序)
        for (from, to) in [(a.clone(), b.clone()), (b.clone(), a.clone())] {
            let pool = pool.clone();
            join.spawn(async move {
                let r = execute_command(
                    &pool,
                    &unique("k-tc2"),
                    &Command::Transfer(Transfer::new(from, to, 100).unwrap()),
                )
                .await;
                if let Err(StoreError::Db(e)) = &r {
                    let code = e.as_database_error().and_then(|d| d.code());
                    if code.as_deref() == Some("40P01") {
                        panic!("检测到死锁(40P01):加锁顺序失效");
                    }
                }
                r
            });
        }
    }
    let mut ok = 0;
    while let Some(r) = join.join_next().await {
        if r.unwrap().is_ok() {
            ok += 1;
        }
    }
    assert_eq!(ok, 200, "资金充足,双向互转应全部成功");

    let va = queries::get_account(&pool, &a).await.unwrap().unwrap();
    let vb = queries::get_account(&pool, &b).await.unwrap().unwrap();
    assert_eq!(
        va.balance_cents + vb.balance_cents,
        200_000,
        "互转不改变总量"
    );
    assert_reconciled(&pool, "T-C2").await;
}

// ---------- T-C3 混合风暴:转账 + 取款抢同一账户,不超扣 ----------

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn tc3_混合风暴_转账与取款竞争() {
    let Some(pool) = test_pool().await else {
        eprintln!("SKIP: 缺 BANK_DB_* 环境变量");
        return;
    };
    let a = open(&pool, "TC3-A").await;
    let b = open(&pool, "TC3-B").await;
    deposit(&pool, &a, 1_000).await; // 只够 10 笔 100 分

    let mut join = tokio::task::JoinSet::new();
    for i in 0..30 {
        let pool = pool.clone();
        let a2 = a.clone();
        let b2 = b.clone();
        join.spawn(async move {
            let cmd = if i % 2 == 0 {
                Command::Transfer(Transfer::new(a2, b2, 100).unwrap())
            } else {
                Command::Withdraw(Withdraw::new(a2, 100).unwrap())
            };
            execute_command(&pool, &unique("k-tc3"), &cmd).await
        });
    }
    let mut ok = 0;
    while let Some(r) = join.join_next().await {
        match r.unwrap() {
            Ok(_) => ok += 1,
            Err(StoreError::InsufficientFunds(_)) => {}
            Err(e) => panic!("意外错误: {e:?}"),
        }
    }
    let va = queries::get_account(&pool, &a).await.unwrap().unwrap();
    assert_eq!(ok, 10, "1000 分只够 10 笔 100 分");
    assert_eq!(
        va.balance_cents,
        1_000 - ok * 100,
        "扣款总额必须与成功笔数吻合"
    );
    assert!(va.balance_cents >= 0, "绝不出现负余额");
    assert_reconciled(&pool, "T-C3").await;
}
