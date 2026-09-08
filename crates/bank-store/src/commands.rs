//! 命令执行事务脚本。
//!
//! 每个命令一个 PG 事务,流程(见 PROJECT_PLAN §2):
//!   1. 幂等领取(INSERT ON CONFLICT + 行锁;冲突已提交 → 重放原结果)
//!   2. 按 account_no 排序锁账户(防死锁;锁粒度 = 未来分区键)
//!   3. 领域校验(状态 / 余额 / 现金池)
//!   4. 追加分录(不可变)+ 更新余额投影(账户 / 科目,同事务)
//!   5. commit;业务失败整事务回滚(命令行随回滚消失,同键可重试)
//!
//! 失败不留痕:只有 committed 才落命令行;进程在 commit 后应答前被杀 → 行已
//! committed,客户端同键重试即得"重放原结果" —— outcome_unknown 的收敛路径
//! (测试 T-E2 覆盖)。

use std::collections::HashMap;

use bank_domain::{Command, DomainError, LedgerSide, OpenAccount, Transfer};
use serde_json::{json, Value};
use sqlx::{PgPool, Postgres, Transaction};

use crate::error::StoreError;

/// 命令执行结果
#[derive(Debug, Clone)]
pub struct ExecOutcome {
    /// true = 幂等重放(返回历史已提交结果,未重复记账)
    pub replay: bool,
    /// 命令结果(随命令账落库,重放时原样返回)
    pub result: Value,
}

const CASH_COA_CODE: &str = "1001";
const DEPOSIT_COA_CODE: &str = "2001";
const CLAIM_MAX_ATTEMPTS: u32 = 5;

// ---------- 内部行结构 ----------

struct AccountLocked {
    id: i64,
    account_no: String,
    coa_id: i64,
    status: String,
    balance_cents: i64,
    /// 账户所属科目的 normal_side(账户投影按科目方向折算:credit-normal 下 credit 增余额)
    coa_normal_side: String,
}

struct CoaRow {
    id: i64,
    normal_side: String,
}

struct EntryPlan {
    seq: i16,
    coa_id: i64,
    account_id: Option<i64>,
    side: LedgerSide,
    amount_cents: i64,
    /// 该记账位科目的 normal_side(投影折算用)
    coa_normal_side: String,
}

/// 带符号增量:正数 = 该科目 normal 方向上的余额增加。
/// credit-normal 科目:credit +amount / debit -amount;debit-normal 反之。
fn signed_delta(normal_side: &str, side: LedgerSide, amount: i64) -> i64 {
    if side.as_str() == normal_side {
        amount
    } else {
        -amount
    }
}

fn is_unique_violation(e: &sqlx::Error) -> bool {
    matches!(
        e.as_database_error()
            .map(|d| d.code().as_deref() == Some("23505")),
        Some(true)
    )
}

async fn coa_by_code<'a>(
    tx: &mut Transaction<'a, Postgres>,
    code: &str,
) -> Result<CoaRow, StoreError> {
    let row: Option<(i64, String)> =
        sqlx::query_as("SELECT id, normal_side FROM coa WHERE code = $1")
            .bind(code)
            .fetch_optional(&mut **tx)
            .await?;
    row.map(|(id, normal_side)| CoaRow { id, normal_side })
        .ok_or_else(|| StoreError::Internal(format!("缺少科目 {code}")))
}

/// 锁定账户(WHERE ... ORDER BY account_no FOR UPDATE,排序防死锁);缺失即报错
async fn lock_accounts<'a>(
    tx: &mut Transaction<'a, Postgres>,
    account_nos: &[&str],
) -> Result<Vec<AccountLocked>, StoreError> {
    let rows: Vec<(i64, String, i64, String, i64, String)> = sqlx::query_as(
        "SELECT a.id, a.account_no, a.coa_id, a.status, a.balance_cents, c.normal_side AS coa_normal_side
           FROM account a
           JOIN coa c ON c.id = a.coa_id
          WHERE a.account_no = ANY($1)
          ORDER BY a.account_no
          FOR UPDATE",
    )
    .bind(account_nos)
    .fetch_all(&mut **tx)
    .await?;
    if rows.len() != account_nos.len() {
        let missing = account_nos
            .iter()
            .find(|no| !rows.iter().any(|r| r.1 == **no))
            .unwrap_or(&"");
        return Err(StoreError::NotFound(missing.to_string()));
    }
    Ok(rows
        .into_iter()
        .map(
            |(id, account_no, coa_id, status, balance_cents, coa_normal_side)| AccountLocked {
                id,
                account_no,
                coa_id,
                status,
                balance_cents,
                coa_normal_side,
            },
        )
        .collect())
}

fn require_active(acc: &AccountLocked) -> Result<(), StoreError> {
    if acc.status == "active" {
        Ok(())
    } else {
        Err(StoreError::AccountNotActive(acc.account_no.clone()))
    }
}

/// 命令 → 落库 payload(命令账审计 / 事件源用)
fn payload_of(cmd: &Command) -> Value {
    match cmd {
        Command::OpenAccount(o) => json!({ "account_no": o.account_no, "holder": o.holder }),
        Command::Deposit(d) => {
            json!({ "account_no": d.account_no, "amount_cents": d.amount.cents() })
        }
        Command::Withdraw(w) => {
            json!({ "account_no": w.account_no, "amount_cents": w.amount.cents() })
        }
        Command::Transfer(t) => json!({
            "from_account_no": t.from_account_no,
            "to_account_no": t.to_account_no,
            "amount_cents": t.amount.cents(),
        }),
    }
}

// ---------- 命令执行 ----------

pub async fn execute_command(
    pool: &PgPool,
    idempotency_key: &str,
    cmd: &Command,
) -> Result<ExecOutcome, StoreError> {
    let key = idempotency_key.trim();
    if key.is_empty() || key.len() > 128 {
        return Err(StoreError::Domain(DomainError::InvalidArgument(
            "幂等键必须为 1..=128 字符的非空串".into(),
        )));
    }
    let payload = payload_of(cmd);
    let mut tx = pool.begin().await?;

    // 1. 幂等领取:Some = 已提交过,直接重放
    let (command_id, replay_result) = claim_command(&mut tx, key, cmd, &payload).await?;
    if let Some(result) = replay_result {
        tx.commit().await?;
        return Ok(ExecOutcome {
            replay: true,
            result,
        });
    }

    // 2-4. 执行(校验 + 分录 + 投影)
    let result = match cmd {
        Command::OpenAccount(o) => execute_open_account(&mut tx, command_id, o).await?,
        Command::Deposit(d) => execute_deposit(&mut tx, command_id, d).await?,
        Command::Withdraw(w) => execute_withdraw(&mut tx, command_id, w).await?,
        Command::Transfer(t) => execute_transfer(&mut tx, command_id, t).await?,
    };

    sqlx::query(
        "UPDATE command_log SET status = 'committed', result = $2::jsonb, committed_at = now() WHERE id = $1",
    )
    .bind(command_id)
    .bind(&result)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(ExecOutcome {
        replay: false,
        result,
    })
}

/// 幂等领取命令行。返回 (command_id, Option<重放结果>);行锁串行化"接管"决策。
async fn claim_command<'a>(
    tx: &mut Transaction<'a, Postgres>,
    key: &str,
    cmd: &Command,
    payload: &Value,
) -> Result<(i64, Option<Value>), StoreError> {
    for _ in 0..CLAIM_MAX_ATTEMPTS {
        let inserted: Option<i64> = sqlx::query_scalar(
            "INSERT INTO command_log (idempotency_key, command_type, payload, status)
             VALUES ($1, $2, $3::jsonb, 'received')
             ON CONFLICT (idempotency_key) DO NOTHING
             RETURNING id",
        )
        .bind(key)
        .bind(cmd.command_type().as_str())
        .bind(payload)
        .fetch_optional(&mut **tx)
        .await?;
        if let Some(id) = inserted {
            return Ok((id, None));
        }
        // 冲突:等胜者提交 / 回滚后读状态
        let row: Option<(i64, String, Option<Value>)> = sqlx::query_as(
            "SELECT id, status, result FROM command_log WHERE idempotency_key = $1 FOR UPDATE",
        )
        .bind(key)
        .fetch_optional(&mut **tx)
        .await?;
        match row {
            Some((id, status, result)) => {
                if status == "committed" {
                    return Ok((id, Some(result.unwrap_or_else(|| json!({})))));
                }
                return Ok((id, None)); // received / outcome_unknown / failed:接管执行
            }
            None => continue, // 原行被回滚消失:重试插入
        }
    }
    Err(StoreError::Internal("幂等领取重试耗尽".into()))
}

async fn insert_entries<'a>(
    tx: &mut Transaction<'a, Postgres>,
    command_id: i64,
    plans: &[EntryPlan],
) -> Result<(), StoreError> {
    for p in plans {
        sqlx::query(
            "INSERT INTO ledger_entry (command_id, seq, coa_id, account_id, side, amount_cents)
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(command_id)
        .bind(p.seq)
        .bind(p.coa_id)
        .bind(p.account_id)
        .bind(p.side.as_str())
        .bind(p.amount_cents)
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
}

/// 按分录折算带符号增量,更新账户 / 科目余额投影(同事务,CHECK >= 0 兜底)
async fn apply_projection<'a>(
    tx: &mut Transaction<'a, Postgres>,
    plans: &[EntryPlan],
) -> Result<(), StoreError> {
    let mut account_delta: HashMap<i64, i64> = HashMap::new();
    let mut coa_delta: HashMap<i64, i64> = HashMap::new();
    for p in plans {
        let delta = signed_delta(&p.coa_normal_side, p.side, p.amount_cents);
        if let Some(acc_id) = p.account_id {
            *account_delta.entry(acc_id).or_insert(0) += delta;
        }
        *coa_delta.entry(p.coa_id).or_insert(0) += delta;
    }
    for (acc_id, delta) in account_delta {
        if delta != 0 {
            sqlx::query("UPDATE account SET balance_cents = balance_cents + $2 WHERE id = $1")
                .bind(acc_id)
                .bind(delta)
                .execute(&mut **tx)
                .await?;
        }
    }
    for (coa_id, delta) in coa_delta {
        if delta != 0 {
            sqlx::query(
                "UPDATE coa_balance SET balance_cents = balance_cents + $2 WHERE coa_id = $1",
            )
            .bind(coa_id)
            .bind(delta)
            .execute(&mut **tx)
            .await?;
        }
    }
    Ok(())
}

/// 存款:借 现金(debit-normal,debit +)/ 贷 客户存款(credit-normal,credit +)
async fn execute_open_account<'a>(
    tx: &mut Transaction<'a, Postgres>,
    _command_id: i64,
    o: &OpenAccount,
) -> Result<Value, StoreError> {
    let deposit_coa = coa_by_code(tx, DEPOSIT_COA_CODE).await?;
    let ins = sqlx::query("INSERT INTO account (account_no, holder, coa_id) VALUES ($1, $2, $3)")
        .bind(&o.account_no)
        .bind(&o.holder)
        .bind(deposit_coa.id)
        .execute(&mut **tx)
        .await;
    match ins {
        Err(e) if is_unique_violation(&e) => {
            Err(StoreError::AccountAlreadyExists(o.account_no.clone()))
        }
        Err(e) => Err(e.into()),
        Ok(_) => Ok(json!({
            "account_no": o.account_no,
            "status": "active",
            "balance_cents": 0,
        })),
    }
}

async fn execute_deposit<'a>(
    tx: &mut Transaction<'a, Postgres>,
    command_id: i64,
    d: &bank_domain::Deposit,
) -> Result<Value, StoreError> {
    let amount = d.amount.cents();
    let accs = lock_accounts(tx, &[d.account_no.as_str()]).await?;
    let acc = &accs[0];
    require_active(acc)?;
    let cash = coa_by_code(tx, CASH_COA_CODE).await?;

    let plans = [
        EntryPlan {
            seq: 0,
            coa_id: cash.id,
            account_id: None,
            side: LedgerSide::Debit,
            amount_cents: amount,
            coa_normal_side: cash.normal_side.clone(),
        },
        EntryPlan {
            seq: 1,
            coa_id: acc.coa_id,
            account_id: Some(acc.id),
            side: LedgerSide::Credit,
            amount_cents: amount,
            coa_normal_side: acc.coa_normal_side.clone(),
        },
    ];
    insert_entries(tx, command_id, &plans).await?;
    apply_projection(tx, &plans).await?;
    Ok(json!({ "account_no": d.account_no, "balance_cents": acc.balance_cents + amount }))
}

/// 取款:借 客户存款(debit - 余额)/ 贷 现金(credit - 现金池);两处余额都要够
async fn execute_withdraw<'a>(
    tx: &mut Transaction<'a, Postgres>,
    command_id: i64,
    w: &bank_domain::Withdraw,
) -> Result<Value, StoreError> {
    let amount = w.amount.cents();
    let accs = lock_accounts(tx, &[w.account_no.as_str()]).await?;
    let acc = &accs[0];
    require_active(acc)?;
    if acc.balance_cents < amount {
        return Err(StoreError::InsufficientFunds(acc.account_no.clone()));
    }
    let cash = coa_by_code(tx, CASH_COA_CODE).await?;
    // 锁现金池行并校验(银行不能兑付未持有的现金)
    let cash_balance: Option<i64> =
        sqlx::query_scalar("SELECT balance_cents FROM coa_balance WHERE coa_id = $1 FOR UPDATE")
            .bind(cash.id)
            .fetch_optional(&mut **tx)
            .await?;
    let cash_balance =
        cash_balance.ok_or_else(|| StoreError::Internal("缺少现金池余额行".into()))?;
    if cash_balance < amount {
        return Err(StoreError::InsufficientFunds(format!(
            "现金池({CASH_COA_CODE})"
        )));
    }

    let plans = [
        EntryPlan {
            seq: 0,
            coa_id: acc.coa_id,
            account_id: Some(acc.id),
            side: LedgerSide::Debit,
            amount_cents: amount,
            coa_normal_side: acc.coa_normal_side.clone(),
        },
        EntryPlan {
            seq: 1,
            coa_id: cash.id,
            account_id: None,
            side: LedgerSide::Credit,
            amount_cents: amount,
            coa_normal_side: cash.normal_side.clone(),
        },
    ];
    insert_entries(tx, command_id, &plans).await?;
    apply_projection(tx, &plans).await?;
    Ok(json!({ "account_no": w.account_no, "balance_cents": acc.balance_cents - amount }))
}

/// 转账:借 付款账户(debit -)/ 贷 收款账户(credit +);科目 2001 净额为零,跳过科目更新
async fn execute_transfer<'a>(
    tx: &mut Transaction<'a, Postgres>,
    command_id: i64,
    t: &Transfer,
) -> Result<Value, StoreError> {
    let amount = t.amount.cents();
    let accs = lock_accounts(tx, &[t.from_account_no.as_str(), t.to_account_no.as_str()]).await?;
    let from = accs
        .iter()
        .find(|a| a.account_no == t.from_account_no)
        .ok_or_else(|| StoreError::NotFound(t.from_account_no.clone()))?;
    let to = accs
        .iter()
        .find(|a| a.account_no == t.to_account_no)
        .ok_or_else(|| StoreError::NotFound(t.to_account_no.clone()))?;
    require_active(from)?;
    require_active(to)?;
    if from.balance_cents < amount {
        return Err(StoreError::InsufficientFunds(from.account_no.clone()));
    }

    let plans = [
        EntryPlan {
            seq: 0,
            coa_id: from.coa_id,
            account_id: Some(from.id),
            side: LedgerSide::Debit,
            amount_cents: amount,
            coa_normal_side: from.coa_normal_side.clone(),
        },
        EntryPlan {
            seq: 1,
            coa_id: to.coa_id,
            account_id: Some(to.id),
            side: LedgerSide::Credit,
            amount_cents: amount,
            coa_normal_side: to.coa_normal_side.clone(),
        },
    ];
    insert_entries(tx, command_id, &plans).await?;
    apply_projection(tx, &plans).await?;
    Ok(json!({
        "from_account_no": t.from_account_no,
        "to_account_no": t.to_account_no,
        "from_balance_cents": from.balance_cents - amount,
        "to_balance_cents": to.balance_cents + amount,
    }))
}
