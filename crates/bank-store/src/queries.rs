//! 查询:账户 / 流水 / 命令状态 / 科目试算(只读,走投影,不锁)

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::PgPool;

use crate::error::StoreError;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountView {
    pub account_no: String,
    pub holder: Option<String>,
    pub account_type: String,
    pub currency: String,
    pub status: String,
    pub balance_cents: i64,
    pub opened_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntryView {
    pub id: i64,
    pub command_id: i64,
    pub command_type: String,
    pub seq: i16,
    pub side: String,
    pub amount_cents: i64,
    pub booked_at: String,
    pub reverses_command_id: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandView {
    pub command_type: String,
    pub status: String,
    pub result: Option<Value>,
    pub created_at: String,
    pub committed_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BalanceSheetRow {
    pub code: String,
    pub name: String,
    pub normal_side: String,
    pub balance_cents: i64,
}

// ---------- 查询行类型别名 ----------

type AccountRow = (String, Option<String>, String, String, String, i64, String);
type EntryRow = (i64, i64, String, i16, String, i64, String, Option<i64>);
type CommandRow = (String, String, Option<Value>, String, Option<String>);
type SheetRow = (String, String, String, i64);

pub async fn get_account(
    pool: &PgPool,
    account_no: &str,
) -> Result<Option<AccountView>, StoreError> {
    let row: Option<AccountRow> = sqlx::query_as(
        "SELECT a.account_no, a.holder, a.type AS account_type, a.currency, a.status,
                a.balance_cents, a.opened_at::text AS opened_at
           FROM account a
          WHERE a.account_no = $1",
    )
    .bind(account_no)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(
        |(account_no, holder, account_type, currency, status, balance_cents, opened_at)| {
            AccountView {
                account_no,
                holder,
                account_type,
                currency,
                status,
                balance_cents,
                opened_at,
            }
        },
    ))
}

pub async fn get_entries(
    pool: &PgPool,
    account_no: &str,
    limit: i64,
) -> Result<Vec<EntryView>, StoreError> {
    let limit = limit.clamp(1, 500);
    let rows: Vec<EntryRow> = sqlx::query_as(
        "SELECT e.id, e.command_id, c.command_type, e.seq, e.side, e.amount_cents,
                e.booked_at::text AS booked_at, e.reverses_command_id
           FROM ledger_entry e
           JOIN command_log c ON c.id = e.command_id
          WHERE e.account_id = (SELECT id FROM account WHERE account_no = $1)
          ORDER BY e.id DESC
          LIMIT $2",
    )
    .bind(account_no)
    .bind(limit)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(
            |(
                id,
                command_id,
                command_type,
                seq,
                side,
                amount_cents,
                booked_at,
                reverses_command_id,
            )| {
                EntryView {
                    id,
                    command_id,
                    command_type,
                    seq,
                    side,
                    amount_cents,
                    booked_at,
                    reverses_command_id,
                }
            },
        )
        .collect())
}

pub async fn get_command(
    pool: &PgPool,
    idempotency_key: &str,
) -> Result<Option<CommandView>, StoreError> {
    let row: Option<CommandRow> = sqlx::query_as(
        "SELECT command_type, status, result, created_at::text AS created_at,
                committed_at::text AS committed_at
           FROM command_log
          WHERE idempotency_key = $1",
    )
    .bind(idempotency_key)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(
        |(command_type, status, result, created_at, committed_at)| CommandView {
            command_type,
            status,
            result,
            created_at,
            committed_at,
        },
    ))
}

/// 科目试算表(正数 = normal 方向余额;平衡时 Σ debit-normal = Σ credit-normal)
pub async fn balance_sheet(pool: &PgPool) -> Result<Vec<BalanceSheetRow>, StoreError> {
    let rows: Vec<SheetRow> = sqlx::query_as(
        "SELECT c.code, c.name, c.normal_side, cb.balance_cents
           FROM coa_balance cb
           JOIN coa c ON c.id = cb.coa_id
          ORDER BY c.code",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(code, name, normal_side, balance_cents)| BalanceSheetRow {
            code,
            name,
            normal_side,
            balance_cents,
        })
        .collect())
}
