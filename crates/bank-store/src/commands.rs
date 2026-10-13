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

use bank_domain::{
    Command, DomainError, LedgerSide, OpenAccount, Reverse, RunEod, SettleInterest, Transfer,
};
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

/// 锁定账户(严格按 account_no 升序**逐个**加锁 → 全局加锁顺序,杜绝死锁);缺失即报错。
///
/// 两个踩过的坑:
/// 1. `WHERE no = ANY($1) ORDER BY no FOR UPDATE` **不保证**加锁顺序 = ORDER BY 顺序
///    (锁在行产出过程中获取),并发随机账户对会成环死锁 —— 必须逐个按序加锁。
/// 2. 不 JOIN 参考表 coa:FOR UPDATE 要求对涉及表有 UPDATE 权限,而 coa 只授予 SELECT。
async fn lock_accounts<'a>(
    tx: &mut Transaction<'a, Postgres>,
    account_nos: &[&str],
) -> Result<Vec<AccountLocked>, StoreError> {
    let mut sorted: Vec<String> = account_nos.iter().map(|s| s.to_string()).collect();
    sorted.sort();
    sorted.dedup();

    let mut locked = Vec::with_capacity(sorted.len());
    for no in &sorted {
        let row: Option<(i64, String, i64, String, i64)> = sqlx::query_as(
            "SELECT a.id, a.account_no, a.coa_id, a.status, a.balance_cents
               FROM account a
              WHERE a.account_no = $1
              FOR UPDATE",
        )
        .bind(no)
        .fetch_optional(&mut **tx)
        .await?;
        let (id, account_no, coa_id, status, balance_cents) =
            row.ok_or_else(|| StoreError::NotFound(no.clone()))?;
        locked.push(AccountLocked {
            id,
            account_no,
            coa_id,
            status,
            balance_cents,
            coa_normal_side: String::new(), // 稍后填充
        });
    }

    let coa_ids: Vec<i64> = locked.iter().map(|a| a.coa_id).collect();
    let coas: Vec<(i64, String)> =
        sqlx::query_as("SELECT id, normal_side FROM coa WHERE id = ANY($1)")
            .bind(&coa_ids)
            .fetch_all(&mut **tx)
            .await?;
    let normal: HashMap<i64, String> = coas.into_iter().collect();
    for acc in &mut locked {
        acc.coa_normal_side = normal.get(&acc.coa_id).cloned().ok_or_else(|| {
            StoreError::Internal(format!(
                "账户 {} 引用科目 {} 不存在",
                acc.account_no, acc.coa_id
            ))
        })?;
    }
    Ok(locked)
}

/// 按账户 id 升序逐个加锁(冲正/计息路径,同样保证全局加锁顺序)
async fn lock_accounts_by_ids<'a>(
    tx: &mut Transaction<'a, Postgres>,
    ids: &[i64],
) -> Result<(), StoreError> {
    let mut sorted: Vec<i64> = ids.to_vec();
    sorted.sort_unstable();
    sorted.dedup();
    for id in sorted {
        sqlx::query("SELECT 1 FROM account WHERE id = $1 FOR UPDATE")
            .bind(id)
            .fetch_optional(&mut **tx)
            .await?;
    }
    Ok(())
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
        Command::Reverse(r) => json!({ "target_idempotency_key": r.target_idempotency_key }),
        Command::SettleInterest(s) => json!({
            "period_start": s.period_start,
            "period_end": s.period_end,
            "annual_rate_bp": s.annual_rate_bp,
        }),
        Command::RunEod(e) => json!({ "day": e.day }),
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

    // 2-4. 执行(校验 + 分录 + 投影);Reverse 额外登记"冲正了哪条原命令"
    let (result, reverses_target) = match cmd {
        Command::OpenAccount(o) => (execute_open_account(&mut tx, command_id, o).await?, None),
        Command::Deposit(d) => (execute_deposit(&mut tx, command_id, d).await?, None),
        Command::Withdraw(w) => (execute_withdraw(&mut tx, command_id, w).await?, None),
        Command::Transfer(t) => (execute_transfer(&mut tx, command_id, t).await?, None),
        Command::Reverse(r) => {
            let (v, orig_id) = execute_reverse(&mut tx, command_id, r).await?;
            (v, Some(orig_id))
        }
        Command::SettleInterest(s) => {
            (execute_settle_interest(&mut tx, command_id, s).await?, None)
        }
        Command::RunEod(e) => (execute_run_eod(&mut tx, e).await?, None),
    };

    sqlx::query(
        "UPDATE command_log
            SET status = 'committed', result = $2::jsonb, committed_at = now(), reverses_command_id = $3
          WHERE id = $1",
    )
    .bind(command_id)
    .bind(&result)
    .bind(reverses_target)
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
    insert_entries_with_reversal(tx, command_id, plans, None).await
}

/// 追加分录;reverses 为 Some 时记录"本条分录属于对某原命令的冲正"
async fn insert_entries_with_reversal<'a>(
    tx: &mut Transaction<'a, Postgres>,
    command_id: i64,
    plans: &[EntryPlan],
    reverses: Option<i64>,
) -> Result<(), StoreError> {
    for p in plans {
        sqlx::query(
            "INSERT INTO ledger_entry (command_id, seq, coa_id, account_id, side, amount_cents, reverses_command_id)
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(command_id)
        .bind(p.seq)
        .bind(p.coa_id)
        .bind(p.account_id)
        .bind(p.side.as_str())
        .bind(p.amount_cents)
        .bind(reverses)
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
}

/// 按分录折算带符号增量,更新账户 / 科目余额投影(同事务,CHECK >= 0 兜底)。
///
/// 关键:更新顺序必须**全局确定**(按 id 升序),否则一条命令涉及多行投影时
/// (存款/取款改"现金 + 客户存款"两行、计息改多账户)并发下会成环死锁。
/// HashMap 迭代顺序随机 —— 踩过这个坑(压测时表现为 coa_balance 上大量死锁)。
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

    // 确定性顺序:账户按 id 升序 → 科目按 id 升序(与加锁顺序一致)
    let mut acc_items: Vec<(i64, i64)> =
        account_delta.into_iter().filter(|(_, d)| *d != 0).collect();
    acc_items.sort_unstable_by_key(|(id, _)| *id);
    let mut coa_items: Vec<(i64, i64)> = coa_delta.into_iter().filter(|(_, d)| *d != 0).collect();
    coa_items.sort_unstable_by_key(|(id, _)| *id);

    for (acc_id, delta) in acc_items {
        let res =
            sqlx::query("UPDATE account SET balance_cents = balance_cents + $2 WHERE id = $1")
                .bind(acc_id)
                .bind(delta)
                .execute(&mut **tx)
                .await?;
        if res.rows_affected() != 1 {
            return Err(StoreError::Internal(format!(
                "账户投影更新影响了 {} 行(应为 1),id={acc_id}",
                res.rows_affected()
            )));
        }
    }
    for (coa_id, delta) in coa_items {
        let res = sqlx::query(
            "UPDATE coa_balance SET balance_cents = balance_cents + $2 WHERE coa_id = $1",
        )
        .bind(coa_id)
        .bind(delta)
        .execute(&mut **tx)
        .await?;
        if res.rows_affected() != 1 {
            return Err(StoreError::Internal(format!(
                "科目投影更新影响了 {} 行(应为 1),coa_id={coa_id}(投影行缺失会导致静默漂移)",
                res.rows_affected()
            )));
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

// ---------- M3:冲正 / 计息 / 日切 ----------

const INTEREST_EXPENSE_COA_CODE: &str = "6601";
/// 计息分母:年利率以 bp(万分之几)表示,全年 360 天 ⇒ 10_000 × 360
const INTEREST_DENOM: i128 = 10_000 * 360;

/// 冲正:按原分录逐条追加反向分录(原流水永不改动),并引用原命令。
/// 规则(方案 Q21):仅全额、每笔原交易限一次、不允许冲正冲正命令。
/// 若冲正会使账户余额或现金池为负(钱已被花掉)则拒绝,返回明确错误。
async fn execute_reverse<'a>(
    tx: &mut Transaction<'a, Postgres>,
    command_id: i64,
    r: &Reverse,
) -> Result<(Value, i64), StoreError> {
    let orig: Option<(i64, String, String)> = sqlx::query_as(
        "SELECT id, command_type, status FROM command_log WHERE idempotency_key = $1 FOR UPDATE",
    )
    .bind(&r.target_idempotency_key)
    .fetch_optional(&mut **tx)
    .await?;
    let (orig_id, orig_type, orig_status) =
        orig.ok_or_else(|| StoreError::OriginalNotFound(r.target_idempotency_key.clone()))?;
    if orig_status != "committed" {
        return Err(StoreError::NotReversible(format!(
            "{} 状态为 {orig_status}",
            r.target_idempotency_key
        )));
    }
    if orig_type == "open_account" {
        return Err(StoreError::NotReversible("开户命令无分录可冲".into()));
    }
    if orig_type == "reverse" {
        return Err(StoreError::NotReversible("不允许冲正冲正命令".into()));
    }
    let already: Option<i64> =
        sqlx::query_scalar("SELECT id FROM command_log WHERE reverses_command_id = $1 LIMIT 1")
            .bind(orig_id)
            .fetch_optional(&mut **tx)
            .await?;
    if already.is_some() {
        return Err(StoreError::AlreadyReversed(
            r.target_idempotency_key.clone(),
        ));
    }

    let rows: Vec<(i16, i64, Option<i64>, String, i64)> = sqlx::query_as(
        "SELECT seq, coa_id, account_id, side, amount_cents
           FROM ledger_entry WHERE command_id = $1 ORDER BY seq",
    )
    .bind(orig_id)
    .fetch_all(&mut **tx)
    .await?;
    if rows.is_empty() {
        return Err(StoreError::NotReversible("原命令无分录可冲".into()));
    }

    // 锁定涉及账户(排序防死锁)
    let mut acc_ids: Vec<i64> = rows.iter().filter_map(|r| r.2).collect();
    acc_ids.sort_unstable();
    acc_ids.dedup();
    // 锁定涉及账户(按 id 升序逐个加锁,保证全局加锁顺序)
    lock_accounts_by_ids(tx, &acc_ids).await?;
    let account_balances: HashMap<i64, i64> = if acc_ids.is_empty() {
        HashMap::new()
    } else {
        sqlx::query_as::<_, (i64, i64)>("SELECT id, balance_cents FROM account WHERE id = ANY($1)")
            .bind(&acc_ids)
            .fetch_all(&mut **tx)
            .await?
            .into_iter()
            .collect()
    };

    // 科目方向(coa 只读;余额行单独加锁,避免 FOR UPDATE 触到参考表)
    let mut coa_ids: Vec<i64> = rows.iter().map(|r| r.1).collect();
    coa_ids.sort_unstable();
    coa_ids.dedup();
    let normal: HashMap<i64, String> =
        sqlx::query_as::<_, (i64, String)>("SELECT id, normal_side FROM coa WHERE id = ANY($1)")
            .bind(&coa_ids)
            .fetch_all(&mut **tx)
            .await?
            .into_iter()
            .collect();
    // 科目投影行:同样按 coa_id 升序逐个加锁(避免成环)
    let coa_balances: HashMap<i64, i64> = {
        let mut rows = Vec::new();
        for coa_id in &coa_ids {
            let bal: Option<i64> = sqlx::query_scalar(
                "SELECT balance_cents FROM coa_balance WHERE coa_id = $1 FOR UPDATE",
            )
            .bind(coa_id)
            .fetch_optional(&mut **tx)
            .await?;
            rows.push((*coa_id, bal.unwrap_or(0)));
        }
        rows.into_iter().collect()
    };

    // 构造反向分录
    let mut plans = Vec::with_capacity(rows.len());
    for (seq, coa_id, account_id, side, amount_cents) in rows {
        let flipped = if side == "debit" {
            LedgerSide::Credit
        } else {
            LedgerSide::Debit
        };
        let coa_normal_side = normal
            .get(&coa_id)
            .cloned()
            .ok_or_else(|| StoreError::Internal(format!("分录引用科目 {coa_id} 不存在")))?;
        plans.push(EntryPlan {
            seq,
            coa_id,
            account_id,
            side: flipped,
            amount_cents,
            coa_normal_side,
        });
    }

    // 预检:不得把账户或科目余额冲成负数
    let mut acc_delta: HashMap<i64, i64> = HashMap::new();
    let mut coa_delta: HashMap<i64, i64> = HashMap::new();
    for p in &plans {
        let d = signed_delta(&p.coa_normal_side, p.side, p.amount_cents);
        if let Some(a) = p.account_id {
            *acc_delta.entry(a).or_insert(0) += d;
        }
        *coa_delta.entry(p.coa_id).or_insert(0) += d;
    }
    for (acc_id, d) in &acc_delta {
        let bal = account_balances.get(acc_id).copied().unwrap_or(0);
        if bal + d < 0 {
            return Err(StoreError::WouldOverdraw(format!(
                "账户 id={acc_id} 余额 {bal} 分,冲正后将为 {} 分",
                bal + d
            )));
        }
    }
    for (coa_id, d) in &coa_delta {
        let bal = coa_balances.get(coa_id).copied().unwrap_or(0);
        if bal + d < 0 {
            return Err(StoreError::WouldOverdraw(format!(
                "科目 id={coa_id} 余额 {bal} 分,冲正后将为 {} 分",
                bal + d
            )));
        }
    }

    insert_entries_with_reversal(tx, command_id, &plans, Some(orig_id)).await?;
    apply_projection(tx, &plans).await?;
    Ok((
        json!({
            "reversed_command": r.target_idempotency_key,
            "reversed_type": orig_type,
            "entries": plans.len(),
        }),
        orig_id,
    ))
}

/// 区间计息:Σ(日终余额 × 年利率 bp) ÷ (10000 × 360),四舍五入到分,结算日一次入账。
/// 借 6601 利息费用 / 贷 客户存款;同一区间只能结算一次(interest_settlement 唯一约束)。
async fn execute_settle_interest<'a>(
    tx: &mut Transaction<'a, Postgres>,
    command_id: i64,
    s: &SettleInterest,
) -> Result<Value, StoreError> {
    let exists: Option<i32> = sqlx::query_scalar(
        "SELECT 1 FROM interest_settlement WHERE period_start = $1::date AND period_end = $2::date",
    )
    .bind(&s.period_start)
    .bind(&s.period_end)
    .fetch_optional(&mut **tx)
    .await?;
    if exists.is_some() {
        return Err(StoreError::PeriodAlreadySettled(format!(
            "{} → {}",
            s.period_start, s.period_end
        )));
    }
    sqlx::query(
        "INSERT INTO interest_settlement (period_start, period_end, annual_rate_bp)
         VALUES ($1::date, $2::date, $3)",
    )
    .bind(&s.period_start)
    .bind(&s.period_end)
    .bind(s.annual_rate_bp)
    .execute(&mut **tx)
    .await?;

    // 日积数:Σ 区间内各日终余额
    let rows: Vec<(i64, String, i64, i64)> = sqlx::query_as(
        "SELECT a.id, a.account_no, a.coa_id, COALESCE(sum(adb.balance_cents), 0)::bigint AS 积数
           FROM account a
           JOIN account_daily_balance adb ON adb.account_id = a.id
          WHERE adb.day >= $1::date AND adb.day < $2::date
          GROUP BY a.id, a.account_no, a.coa_id
          ORDER BY a.id",
    )
    .bind(&s.period_start)
    .bind(&s.period_end)
    .fetch_all(&mut **tx)
    .await?;

    let expense_coa = coa_by_code(tx, INTEREST_EXPENSE_COA_CODE).await?;
    if rows.is_empty() {
        return Ok(json!({
            "period_start": s.period_start,
            "period_end": s.period_end,
            "annual_rate_bp": s.annual_rate_bp,
            "total_interest_cents": 0,
            "accounts_credited": 0,
            "accounts_considered": 0,
        }));
    }

    // 锁定涉及账户(按 id 升序逐个加锁)
    let acc_ids: Vec<i64> = rows.iter().map(|r| r.0).collect();
    lock_accounts_by_ids(tx, &acc_ids).await?;

    let mut coa_ids: Vec<i64> = rows.iter().map(|r| r.2).collect();
    coa_ids.push(expense_coa.id);
    coa_ids.sort_unstable();
    coa_ids.dedup();
    let normal: HashMap<i64, String> =
        sqlx::query_as::<_, (i64, String)>("SELECT id, normal_side FROM coa WHERE id = ANY($1)")
            .bind(&coa_ids)
            .fetch_all(&mut **tx)
            .await?
            .into_iter()
            .collect();

    let mut plans: Vec<EntryPlan> = Vec::new();
    let mut seq: i16 = 0;
    let mut total: i64 = 0;
    let mut credited: i64 = 0;
    for (acc_id, _account_no, coa_id, 积数) in &rows {
        let numerator = (*积数 as i128) * (s.annual_rate_bp as i128);
        let cents_i128 = (numerator + INTEREST_DENOM / 2) / INTEREST_DENOM; // 四舍五入到分
        if cents_i128 <= 0 {
            continue; // 不足 1 分不计提,避免产生 0 金额分录
        }
        let cents = i64::try_from(cents_i128)
            .map_err(|_| StoreError::Internal("利息金额溢出 i64".into()))?;
        total = total
            .checked_add(cents)
            .ok_or_else(|| StoreError::Internal("利息合计溢出 i64".into()))?;
        credited += 1;
        let acc_normal = normal
            .get(coa_id)
            .cloned()
            .ok_or_else(|| StoreError::Internal(format!("账户科目 {coa_id} 缺失")))?;
        plans.push(EntryPlan {
            seq,
            coa_id: expense_coa.id,
            account_id: None,
            side: LedgerSide::Debit,
            amount_cents: cents,
            coa_normal_side: expense_coa.normal_side.clone(),
        });
        seq += 1;
        plans.push(EntryPlan {
            seq,
            coa_id: *coa_id,
            account_id: Some(*acc_id),
            side: LedgerSide::Credit,
            amount_cents: cents,
            coa_normal_side: acc_normal,
        });
        seq += 1;
    }
    if !plans.is_empty() {
        insert_entries(tx, command_id, &plans).await?;
        apply_projection(tx, &plans).await?;
    }
    Ok(json!({
        "period_start": s.period_start,
        "period_end": s.period_end,
        "annual_rate_bp": s.annual_rate_bp,
        "total_interest_cents": total,
        "accounts_credited": credited,
        "accounts_considered": rows.len(),
    }))
}

/// 轻量日切:指定日期做日终快照 + 试算平衡。closed_at 由 day 推导(可重放)。
async fn execute_run_eod<'a>(
    tx: &mut Transaction<'a, Postgres>,
    e: &RunEod,
) -> Result<Value, StoreError> {
    let exists: Option<i32> = sqlx::query_scalar("SELECT 1 FROM eod_snapshot WHERE day = $1::date")
        .bind(&e.day)
        .fetch_optional(&mut **tx)
        .await?;
    if exists.is_some() {
        return Err(StoreError::DayAlreadyClosed(e.day.clone()));
    }
    let (debit_total, credit_total): (i64, i64) = sqlx::query_as(
        "SELECT COALESCE(sum(cb.balance_cents) FILTER (WHERE c.normal_side = 'debit'), 0)::bigint,
                COALESCE(sum(cb.balance_cents) FILTER (WHERE c.normal_side = 'credit'), 0)::bigint
           FROM coa_balance cb JOIN coa c ON c.id = cb.coa_id",
    )
    .fetch_one(&mut **tx)
    .await?;
    let balanced = debit_total == credit_total;

    sqlx::query(
        "INSERT INTO eod_snapshot (day, closed_at, balanced)
         VALUES ($1::date, ($1::date + time '23:59:59') AT TIME ZONE 'UTC', $2)",
    )
    .bind(&e.day)
    .bind(balanced)
    .execute(&mut **tx)
    .await?;

    let snap = sqlx::query(
        "INSERT INTO account_daily_balance (account_id, day, balance_cents)
         SELECT id, $1::date, balance_cents FROM account",
    )
    .bind(&e.day)
    .execute(&mut **tx)
    .await?;

    Ok(json!({
        "day": e.day,
        "accounts_snapshotted": snap.rows_affected(),
        "balanced": balanced,
        "debit_side_total_cents": debit_total,
        "credit_side_total_cents": credit_total,
    }))
}
