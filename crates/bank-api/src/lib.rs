//! bank-api —— REST 命令入口与查询(v1,过渡 transport)。
//!
//! 统一命令入口 POST /v1/commands(命令 = 未来事件,transport 随时可换)。
//! 拆成 lib + bin 以便集成测试直接驱动 Router(tests/http_smoke.rs)。

use std::net::SocketAddr;

use axum::extract::{Path, Query, State};
use axum::http::header::HeaderValue;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use bank_domain::{
    Command, CommandType, Deposit, OpenAccount, Reverse, RunEod, SettleInterest, Transfer, Withdraw,
};
use bank_store::commands::execute_command;
use bank_store::queries;
use bank_store::StoreError;
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::PgPool;

#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
}

// ---------- 错误 → HTTP ----------

pub struct ApiError {
    pub status: StatusCode,
    pub code: &'static str,
    pub message: String,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(json!({ "error": { "code": self.code, "message": self.message } })),
        )
            .into_response()
    }
}

impl From<StoreError> for ApiError {
    fn from(e: StoreError) -> Self {
        match e {
            StoreError::Domain(de) => ApiError {
                status: StatusCode::BAD_REQUEST,
                code: "invalid_command",
                message: de.to_string(),
            },
            StoreError::NotFound(acc) => ApiError {
                status: StatusCode::NOT_FOUND,
                code: "account_not_found",
                message: format!("账户不存在: {acc}"),
            },
            StoreError::InsufficientFunds(acc) => ApiError {
                status: StatusCode::CONFLICT,
                code: "insufficient_funds",
                message: format!("余额不足: {acc}"),
            },
            StoreError::AccountNotActive(acc) => ApiError {
                status: StatusCode::CONFLICT,
                code: "account_not_active",
                message: format!("账户状态不允许操作: {acc}"),
            },
            StoreError::AccountAlreadyExists(acc) => ApiError {
                status: StatusCode::CONFLICT,
                code: "account_already_exists",
                message: format!("账户已存在: {acc}"),
            },
            StoreError::OriginalNotFound(k) => ApiError {
                status: StatusCode::NOT_FOUND,
                code: "original_command_not_found",
                message: format!("被冲正命令不存在: {k}"),
            },
            StoreError::NotReversible(msg) => ApiError {
                status: StatusCode::CONFLICT,
                code: "not_reversible",
                message: format!("命令不可冲正: {msg}"),
            },
            StoreError::AlreadyReversed(k) => ApiError {
                status: StatusCode::CONFLICT,
                code: "already_reversed",
                message: format!("原交易已被冲正: {k}"),
            },
            StoreError::WouldOverdraw(msg) => ApiError {
                status: StatusCode::CONFLICT,
                code: "reversal_would_overdraw",
                message: format!("冲正会导致透支,拒绝执行: {msg}"),
            },
            StoreError::PeriodAlreadySettled(p) => ApiError {
                status: StatusCode::CONFLICT,
                code: "period_already_settled",
                message: format!("计息区间已结算: {p}"),
            },
            StoreError::DayAlreadyClosed(d) => ApiError {
                status: StatusCode::CONFLICT,
                code: "day_already_closed",
                message: format!("该日已日切: {d}"),
            },
            StoreError::Internal(msg) => ApiError {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                code: "internal_error",
                message: msg,
            },
            StoreError::Db(e) => ApiError {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                code: "db_error",
                message: e.to_string(),
            },
        }
    }
}

// ---------- 请求体解析 ----------

#[derive(Debug, Deserialize)]
struct CommandRequest {
    #[serde(rename = "type")]
    command_type: String,
    idempotency_key: String,
    payload: Value,
}

fn bad_request(msg: impl Into<String>) -> ApiError {
    ApiError {
        status: StatusCode::BAD_REQUEST,
        code: "bad_request",
        message: msg.into(),
    }
}

#[derive(Deserialize)]
struct OpenPayload {
    account_no: String,
    holder: Option<String>,
}

#[derive(Deserialize)]
struct MoneyPayload {
    account_no: String,
    amount_cents: i64,
}

#[derive(Deserialize)]
struct TransferPayload {
    from_account_no: String,
    to_account_no: String,
    amount_cents: i64,
}

#[derive(Deserialize)]
struct ReversePayload {
    target_idempotency_key: String,
}

#[derive(Deserialize)]
struct SettleInterestPayload {
    period_start: String,
    period_end: String,
    annual_rate_bp: i64,
}

#[derive(Deserialize)]
struct RunEodPayload {
    day: String,
}

fn parse_command(req: &CommandRequest) -> Result<Command, ApiError> {
    let cmd_type =
        CommandType::try_from(req.command_type.as_str()).map_err(|e| bad_request(e.to_string()))?;
    let p = &req.payload;
    Ok(match cmd_type {
        CommandType::OpenAccount => {
            let o: OpenPayload = serde_json::from_value(p.clone())
                .map_err(|e| bad_request(format!("payload 非法: {e}")))?;
            Command::OpenAccount(
                OpenAccount::new(o.account_no, o.holder).map_err(|e| bad_request(e.to_string()))?,
            )
        }
        CommandType::Deposit => {
            let d: MoneyPayload = serde_json::from_value(p.clone())
                .map_err(|e| bad_request(format!("payload 非法: {e}")))?;
            Command::Deposit(
                Deposit::new(d.account_no, d.amount_cents)
                    .map_err(|e| bad_request(e.to_string()))?,
            )
        }
        CommandType::Withdraw => {
            let w: MoneyPayload = serde_json::from_value(p.clone())
                .map_err(|e| bad_request(format!("payload 非法: {e}")))?;
            Command::Withdraw(
                Withdraw::new(w.account_no, w.amount_cents)
                    .map_err(|e| bad_request(e.to_string()))?,
            )
        }
        CommandType::Transfer => {
            let t: TransferPayload = serde_json::from_value(p.clone())
                .map_err(|e| bad_request(format!("payload 非法: {e}")))?;
            Command::Transfer(
                Transfer::new(t.from_account_no, t.to_account_no, t.amount_cents)
                    .map_err(|e| bad_request(e.to_string()))?,
            )
        }
        CommandType::Reverse => {
            let r: ReversePayload = serde_json::from_value(p.clone())
                .map_err(|e| bad_request(format!("payload 非法: {e}")))?;
            Command::Reverse(
                Reverse::new(r.target_idempotency_key).map_err(|e| bad_request(e.to_string()))?,
            )
        }
        CommandType::SettleInterest => {
            let s: SettleInterestPayload = serde_json::from_value(p.clone())
                .map_err(|e| bad_request(format!("payload 非法: {e}")))?;
            Command::SettleInterest(
                SettleInterest::new(s.period_start, s.period_end, s.annual_rate_bp)
                    .map_err(|e| bad_request(e.to_string()))?,
            )
        }
        CommandType::RunEod => {
            let e: RunEodPayload = serde_json::from_value(p.clone())
                .map_err(|err| bad_request(format!("payload 非法: {err}")))?;
            Command::RunEod(RunEod::new(e.day).map_err(|e| bad_request(e.to_string()))?)
        }
    })
}

// ---------- 处理器 ----------

async fn health() -> Json<Value> {
    Json(json!({
        "status": "ok",
        "service": "bank-core",
        "version": env!("CARGO_PKG_VERSION"),
    }))
}

async fn handle_command(
    State(state): State<AppState>,
    Json(req): Json<CommandRequest>,
) -> Result<Response, ApiError> {
    if req.idempotency_key.trim().is_empty() || req.idempotency_key.len() > 128 {
        return Err(bad_request("idempotency_key 必须为 1..=128 字符的非空串"));
    }
    let cmd = parse_command(&req)?;
    let out = execute_command(&state.pool, &req.idempotency_key, &cmd).await?;
    let body = Json(json!({
        "status": "committed",
        "replay": out.replay,
        "command_type": cmd.command_type().as_str(),
        "result": out.result,
    }));
    let mut resp = body.into_response();
    resp.headers_mut().insert(
        "x-replayed",
        HeaderValue::from_static(if out.replay { "true" } else { "false" }),
    );
    Ok(resp)
}

async fn get_account(
    State(state): State<AppState>,
    Path(account_no): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let acc = queries::get_account(&state.pool, &account_no)
        .await?
        .ok_or_else(|| ApiError {
            status: StatusCode::NOT_FOUND,
            code: "account_not_found",
            message: format!("账户不存在: {account_no}"),
        })?;
    Ok(Json(json!(acc)))
}

async fn get_entries(
    State(state): State<AppState>,
    Path(account_no): Path<String>,
    Query(params): Query<Vec<(String, String)>>,
) -> Result<Json<Value>, ApiError> {
    let limit: i64 = params
        .iter()
        .find(|(k, _)| k == "limit")
        .and_then(|(_, v)| v.parse().ok())
        .unwrap_or(100);
    if !queries::get_account(&state.pool, &account_no)
        .await?
        .is_some()
    {
        return Err(ApiError {
            status: StatusCode::NOT_FOUND,
            code: "account_not_found",
            message: format!("账户不存在: {account_no}"),
        });
    }
    let entries = queries::get_entries(&state.pool, &account_no, limit).await?;
    Ok(Json(
        json!({ "account_no": account_no, "count": entries.len(), "entries": entries }),
    ))
}

async fn get_command(
    State(state): State<AppState>,
    Path(idempotency_key): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let cmd = queries::get_command(&state.pool, &idempotency_key)
        .await?
        .ok_or_else(|| ApiError {
            status: StatusCode::NOT_FOUND,
            code: "command_not_found",
            message: format!("命令不存在: {idempotency_key}"),
        })?;
    Ok(Json(json!(cmd)))
}

async fn get_balance_sheet(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let rows = queries::balance_sheet(&state.pool).await?;
    let mut debit_sum: i64 = 0;
    let mut credit_sum: i64 = 0;
    for r in &rows {
        if r.normal_side == "debit" {
            debit_sum += r.balance_cents;
        } else {
            credit_sum += r.balance_cents;
        }
    }
    Ok(Json(json!({
        "balanced": debit_sum == credit_sum,
        "debit_side_total_cents": debit_sum,
        "credit_side_total_cents": credit_sum,
        "rows": rows,
    })))
}

/// 组装 Router(供 main 与集成测试共用)
pub fn build_router(pool: PgPool) -> Router {
    let state = AppState { pool };
    Router::new()
        .route("/healthz", get(health))
        .route("/v1/commands", post(handle_command))
        .route("/v1/commands/{idempotency_key}", get(get_command))
        .route("/v1/accounts/{account_no}", get(get_account))
        .route("/v1/accounts/{account_no}/entries", get(get_entries))
        .route("/v1/balance-sheet", get(get_balance_sheet))
        .with_state(state)
}

/// 启动服务(连接 + 迁移 + 监听 127.0.0.1:8080)
pub async fn serve() -> Result<(), Box<dyn std::error::Error>> {
    let pool = bank_store::connect_and_migrate().await?;
    let app = build_router(pool);
    let addr: SocketAddr = "127.0.0.1:8080".parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    println!("bank-api 就绪: http://{addr}/healthz");
    axum::serve(listener, app).await?;
    Ok(())
}
