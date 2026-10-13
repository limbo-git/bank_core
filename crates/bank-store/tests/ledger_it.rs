//! T-B 集成测试(真实 PG,库 = BANK_DB_NAME 的 *_test 变体)。
//!
//! 无 BANK_DB_* 环境变量时跳过(纯 `cargo test` 不依赖 DB)。
//! 用例清单见 PROJECT_PLAN §6 T-B。

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

    // 迁移以属主身份执行(bank_app 无 DDL 权限)
    let owner_url = format!("postgres://{owner_user}:{owner_password}@{host}:{port}/{test_name}");
    let owner_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&owner_url)
        .await
        .ok()?;
    bank_store::migrate(&owner_pool).await.ok()?;
    owner_pool.close().await;

    // 业务连接使用最小权限的应用角色
    let app_url = format!("postgres://{user}:{password}@{host}:{port}/{test_name}");
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(16)
        .connect(&app_url)
        .await
        .ok()?;
    Some(pool)
}

/// 属主连接(仅用于测试中的"人为注入错误"等需要越权改动的场景)
async fn owner_pool() -> Option<PgPool> {
    let host = std::env::var("BANK_DB_HOST").ok()?;
    let port = std::env::var("BANK_DB_PORT").ok()?;
    let name = std::env::var("BANK_DB_NAME").ok()?;
    let owner_user = std::env::var("BANK_DB_OWNER_USER").ok()?;
    let owner_password = std::env::var("BANK_DB_OWNER_PASSWORD").ok()?;
    let test_name = format!("{name}_test");
    let url = format!("postgres://{owner_user}:{owner_password}@{host}:{port}/{test_name}");
    sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .ok()
}

async fn open(pool: &PgPool, tag: &str) -> String {
    let no = unique(tag);
    let cmd =
        Command::OpenAccount(OpenAccount::new(no.clone(), Some("测试客户".to_string())).unwrap());
    let out = execute_command(pool, &unique("key-open"), &cmd)
        .await
        .unwrap();
    assert!(!out.replay);
    no
}

// ---------- T-B1 主链路 ----------

#[tokio::test]
async fn tb1_存取转账_余额流水与试算平衡() {
    let Some(pool) = test_pool().await else {
        eprintln!("SKIP: 缺 BANK_DB_* 环境变量");
        return;
    };
    let a = open(&pool, "TB1-A").await;
    let b = open(&pool, "TB1-B").await;

    // A 存 100.00
    let out = execute_command(
        &pool,
        &unique("k1"),
        &Command::Deposit(Deposit::new(a.clone(), 10000).unwrap()),
    )
    .await
    .unwrap();
    assert!(!out.replay);
    assert_eq!(out.result["balance_cents"], 10000);

    // A 取 30.00
    execute_command(
        &pool,
        &unique("k2"),
        &Command::Withdraw(Withdraw::new(a.clone(), 3000).unwrap()),
    )
    .await
    .unwrap();

    // A → B 转 20.00
    let out = execute_command(
        &pool,
        &unique("k3"),
        &Command::Transfer(Transfer::new(a.clone(), b.clone(), 2000).unwrap()),
    )
    .await
    .unwrap();
    assert_eq!(out.result["from_balance_cents"], 5000);
    assert_eq!(out.result["to_balance_cents"], 2000);

    // 余额断言:A=50.00, B=20.00
    let va = queries::get_account(&pool, &a).await.unwrap().unwrap();
    let vb = queries::get_account(&pool, &b).await.unwrap().unwrap();
    assert_eq!(va.balance_cents, 5000);
    assert_eq!(vb.balance_cents, 2000);
    assert_eq!(va.status, "active");

    // 流水:A 应有三笔(存/取/转出)
    let ea = queries::get_entries(&pool, &a, 100).await.unwrap();
    assert_eq!(ea.len(), 3);
    let sides: Vec<&str> = ea.iter().map(|e| e.side.as_str()).collect();
    assert_eq!(sides, vec!["debit", "debit", "credit"]); // id 倒序:转出 debit/取款 debit/存款 credit

    // 试算平衡不变式:Σ debit-normal 科目余额 == Σ credit-normal 科目余额
    // (注意:计息入账会同时抬高 6601 利息费用与 2001 存款,故不能用"现金=存款"这种窄等式;
    //  金额也只做下界断言——集成测试并行共享一库,其他用例的流水已计入)
    let sheet = queries::balance_sheet(&pool).await.unwrap();
    let debit_total: i64 = sheet
        .iter()
        .filter(|r| r.normal_side == "debit")
        .map(|r| r.balance_cents)
        .sum();
    let credit_total: i64 = sheet
        .iter()
        .filter(|r| r.normal_side == "credit")
        .map(|r| r.balance_cents)
        .sum();
    assert!(
        debit_total >= 7000 && credit_total >= 7000,
        "至少含本用例的 7000 分: 借 {debit_total} / 贷 {credit_total}"
    );
    assert_eq!(
        debit_total, credit_total,
        "试算平衡:Σ借方科目余额必须等于 Σ贷方科目余额"
    );
}

// ---------- T-B2 失败路径 ----------

#[tokio::test]
async fn tb2_失败路径() {
    let Some(pool) = test_pool().await else {
        eprintln!("SKIP: 缺 BANK_DB_* 环境变量");
        return;
    };
    let a = open(&pool, "TB2-A").await;
    let missing = unique("TB2-NONE");

    // 余额不足
    let err = execute_command(
        &pool,
        &unique("k"),
        &Command::Withdraw(Withdraw::new(a.clone(), 1).unwrap()),
    )
    .await
    .unwrap_err();
    assert!(matches!(err, StoreError::InsufficientFunds(_)));

    // 账户不存在(取款/转账目标)
    let err = execute_command(
        &pool,
        &unique("k"),
        &Command::Withdraw(Withdraw::new(missing.clone(), 1).unwrap()),
    )
    .await
    .unwrap_err();
    assert!(matches!(err, StoreError::NotFound(_)));
    let err = execute_command(
        &pool,
        &unique("k"),
        &Command::Transfer(Transfer::new(a.clone(), missing, 1).unwrap()),
    )
    .await
    .unwrap_err();
    assert!(matches!(err, StoreError::NotFound(_)));

    // 金额非法(构造层即拒)
    assert!(Deposit::new(&a, 0).is_err());
    assert!(Withdraw::new(&a, -1).is_err());

    // 冻结账户不可操作
    let fz = open(&pool, "TB2-FZ").await;
    sqlx::query("UPDATE account SET status = 'frozen' WHERE account_no = $1")
        .bind(&fz)
        .execute(&pool)
        .await
        .unwrap();
    let err = execute_command(
        &pool,
        &unique("k"),
        &Command::Deposit(Deposit::new(fz.clone(), 100).unwrap()),
    )
    .await
    .unwrap_err();
    assert!(matches!(err, StoreError::AccountNotActive(_)));

    // 重复开户(不同命令键同账号)
    let dup = open(&pool, "TB2-DUP").await;
    let err = execute_command(
        &pool,
        &unique("k"),
        &Command::OpenAccount(OpenAccount::new(dup, None).unwrap()),
    )
    .await
    .unwrap_err();
    assert!(matches!(err, StoreError::AccountAlreadyExists(_)));
}

// ---------- T-B3 幂等 ----------

#[tokio::test]
async fn tb3_幂等重发_顺序与并发同键() {
    let Some(pool) = test_pool().await else {
        eprintln!("SKIP: 缺 BANK_DB_* 环境变量");
        return;
    };
    let a = open(&pool, "TB3-A").await;
    let key = unique("TB3-KEY");

    // 顺序重发:两次同样命令,账只记一次
    let cmd = || Command::Deposit(Deposit::new(a.clone(), 500).unwrap());
    let first = execute_command(&pool, &key, &cmd()).await.unwrap();
    assert!(!first.replay);
    let second = execute_command(&pool, &key, &cmd()).await.unwrap();
    assert!(second.replay);
    assert_eq!(first.result, second.result);
    let va = queries::get_account(&pool, &a).await.unwrap().unwrap();
    assert_eq!(va.balance_cents, 500); // 只加了一次

    // 并发同键:8 个不同金额同键齐发 → 恰一个真执行,其余全部重放同一结果
    let key2 = unique("TB3-KEY2");
    let mut join = tokio::task::JoinSet::new();
    for amt in [100_i64, 200, 300, 400, 500, 600, 700, 800] {
        let pool = pool.clone();
        let acc = a.clone();
        let k = key2.clone();
        join.spawn(async move {
            execute_command(
                &pool,
                &k,
                &Command::Deposit(Deposit::new(acc, amt).unwrap()),
            )
            .await
            .unwrap()
        });
    }
    let mut results = vec![];
    while let Some(r) = join.join_next().await {
        results.push(r.unwrap());
    }
    assert_eq!(results.len(), 8);
    let executed = results.iter().filter(|r| !r.replay).count();
    let replayed = results.iter().filter(|r| r.replay).count();
    assert_eq!(executed, 1, "同键并发必须恰一次真实记账");
    assert_eq!(replayed, 7);
    // 所有结果一致(重放返回的就是胜者的结果)
    let first_result = results[0].result.clone();
    assert!(results.iter().all(|r| r.result == first_result));
    // 命令行只有一行
    let (cnt,): (i64,) =
        sqlx::query_as("SELECT count(*) FROM command_log WHERE idempotency_key = $1")
            .bind(&key2)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(cnt, 1);
    // 余额与胜者结果吻合(结果里已含此前的 500 基数)
    let va = queries::get_account(&pool, &a).await.unwrap().unwrap();
    assert_eq!(
        va.balance_cents,
        first_result["balance_cents"].as_i64().unwrap(),
        "最终余额必须等于胜者返回的余额(含基数)"
    );
}

// ---------- T-B4 分录不可变 / 自转账 ----------

#[tokio::test]
async fn tb4_自转账被拒_分录不可变() {
    let Some(pool) = test_pool().await else {
        eprintln!("SKIP: 缺 BANK_DB_* 环境变量");
        return;
    };
    let Some(owner) = owner_pool().await else {
        eprintln!("SKIP: 缺 BANK_DB_OWNER_* 环境变量");
        return;
    };
    let a = open(&pool, "TB4-A").await;

    // 自转账在构造层拒绝
    assert!(Transfer::new(a.clone(), a.clone(), 100).is_err());

    // 落一笔真实分录
    execute_command(
        &pool,
        &unique("k"),
        &Command::Deposit(Deposit::new(a.clone(), 100).unwrap()),
    )
    .await
    .unwrap();
    let (eid,): (i64,) = sqlx::query_as("SELECT id FROM ledger_entry ORDER BY id DESC LIMIT 1")
        .fetch_one(&pool)
        .await
        .unwrap();

    // 应用角色:权限层先挡住
    let app_err =
        sqlx::query("UPDATE ledger_entry SET amount_cents = amount_cents + 1 WHERE id = $1")
            .bind(eid)
            .execute(&pool)
            .await
            .unwrap_err();
    assert!(
        app_err.to_string().contains("权限") || app_err.to_string().contains("permission"),
        "应用角色的改分录应被权限拒绝: {app_err}"
    );

    // 属主(权限上合法):触发器必须拒绝,且信息里点明"不可变"
    let owner_err =
        sqlx::query("UPDATE ledger_entry SET amount_cents = amount_cents + 1 WHERE id = $1")
            .bind(eid)
            .execute(&owner)
            .await
            .unwrap_err();
    assert!(
        owner_err.to_string().contains("不可变"),
        "属主改分录应被触发器拒绝: {owner_err}"
    );
    let owner_del = sqlx::query("DELETE FROM ledger_entry WHERE id = $1")
        .bind(eid)
        .execute(&owner)
        .await
        .unwrap_err();
    assert!(
        owner_del.to_string().contains("不可变"),
        "属主删分录应被触发器拒绝: {owner_del}"
    );
}
