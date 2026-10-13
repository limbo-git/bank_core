//! HTTP 层冒烟测试:直接驱动 axum Router(tower::oneshot),不占端口。
//! 补上"M1 审计"里发现的缺口:REST 只有手工 curl、没有自动化测试。

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use sqlx::PgPool;
use tower::ServiceExt;

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

async fn pools() -> Option<PgPool> {
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
    owner.close().await;

    sqlx::postgres::PgPoolOptions::new()
        .max_connections(8)
        .connect(&format!(
            "postgres://{user}:{password}@{host}:{port}/{test_name}"
        ))
        .await
        .ok()
}

/// 隔离的重放库连接(日切/计息是全局操作,必须与其他用例隔离)
async fn replay_pool() -> Option<PgPool> {
    let host = std::env::var("BANK_DB_HOST").ok()?;
    let port = std::env::var("BANK_DB_PORT").ok()?;
    let name = std::env::var("BANK_DB_NAME").ok()?;
    let user = std::env::var("BANK_DB_USER").ok()?;
    let password = std::env::var("BANK_DB_PASSWORD").ok()?;
    let owner_user = std::env::var("BANK_DB_OWNER_USER").ok()?;
    let owner_password = std::env::var("BANK_DB_OWNER_PASSWORD").ok()?;
    let replay = format!("{name}_replay");

    let owner = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&format!(
            "postgres://{owner_user}:{owner_password}@{host}:{port}/{replay}"
        ))
        .await
        .ok()?;
    bank_store::migrate(&owner).await.ok()?;
    owner.close().await;

    sqlx::postgres::PgPoolOptions::new()
        .max_connections(8)
        .connect(&format!(
            "postgres://{user}:{password}@{host}:{port}/{replay}"
        ))
        .await
        .ok()
}

/// 返回 (状态码, x-replayed 头, JSON body)
async fn send(
    router: &axum::Router,
    method: &str,
    uri: &str,
    body: Option<Value>,
) -> (StatusCode, Option<String>, Value) {
    let builder = Request::builder().method(method).uri(uri);
    let req = match body {
        Some(v) => builder
            .header("content-type", "application/json")
            .body(Body::from(v.to_string()))
            .unwrap(),
        None => builder.body(Body::empty()).unwrap(),
    };
    let resp = router.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let replay = resp
        .headers()
        .get("x-replayed")
        .map(|v| v.to_str().unwrap().to_string());
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let val: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, replay, val)
}

fn cmd(body: Value) -> Value {
    body
}

#[tokio::test]
async fn http_冒烟_全链路_错误码_幂等重放() {
    let Some(pool) = pools().await else {
        eprintln!("SKIP: 缺 BANK_DB_* 环境变量");
        return;
    };
    let router = bank_api::build_router(pool);
    let a = unique("HTTP-A");
    let b = unique("HTTP-B");

    // 健康检查
    let (st, _, body) = send(&router, "GET", "/healthz", None).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(body["status"], "ok");

    // 开户 + 存款
    let key_open = unique("k-open");
    let (st, replay, body) = send(
        &router,
        "POST",
        "/v1/commands",
        Some(cmd(json!({"type":"open_account","idempotency_key":key_open,"payload":{"account_no":a,"holder":"HTTP 冒烟"}}))),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(replay.as_deref(), Some("false"));
    assert_eq!(body["result"]["status"], "active");

    let key_dep = unique("k-dep");
    let (st, _, body) = send(
        &router,
        "POST",
        "/v1/commands",
        Some(cmd(json!({"type":"deposit","idempotency_key":key_dep,"payload":{"account_no":a,"amount_cents":10000}}))),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(body["result"]["balance_cents"], 10000);

    // 转入账户开户(转账目标必须存在)
    let (st, _, _) = send(
        &router,
        "POST",
        "/v1/commands",
        Some(cmd(json!({"type":"open_account","idempotency_key":unique("k-open-b"),"payload":{"account_no":b}}))),
    )
    .await;
    assert_eq!(st, StatusCode::OK);

    // 幂等重放:同键换金额 → 返回原结果 + x-replayed: true
    let (st, replay, body) = send(
        &router,
        "POST",
        "/v1/commands",
        Some(cmd(json!({"type":"deposit","idempotency_key":key_dep,"payload":{"account_no":a,"amount_cents":999999}}))),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(replay.as_deref(), Some("true"), "重复键必须标记为重放");
    assert_eq!(body["result"]["balance_cents"], 10000, "重放必须返回原结果");

    // 查询余额 / 流水 / 命令状态
    let (st, _, body) = send(&router, "GET", &format!("/v1/accounts/{a}"), None).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(body["balance_cents"], 10000);
    let (st, _, body) = send(
        &router,
        "GET",
        &format!("/v1/accounts/{a}/entries?limit=10"),
        None,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(body["count"], 1, "存款一笔分录只应有一条(客户侧)");
    let (st, _, body) = send(&router, "GET", &format!("/v1/commands/{key_dep}"), None).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(body["status"], "committed");

    // 转账 A → B
    let (st, _, body) = send(
        &router,
        "POST",
        "/v1/commands",
        Some(cmd(json!({"type":"transfer","idempotency_key":unique("k-trf"),"payload":{"from_account_no":a,"to_account_no":b,"amount_cents":2000}}))),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(body["result"]["from_balance_cents"], 8000);

    // 错误码矩阵
    let (st, _, _) = send(&router, "GET", "/v1/accounts/NO-SUCH-ACCOUNT", None).await;
    assert_eq!(st, StatusCode::NOT_FOUND, "未知账户应 404");
    let (st, _, _) = send(
        &router,
        "POST",
        "/v1/commands",
        Some(cmd(json!({"type":"withdraw","idempotency_key":unique("k-w"),"payload":{"account_no":a,"amount_cents":99999999}}))),
    )
    .await;
    assert_eq!(st, StatusCode::CONFLICT, "余额不足应 409");
    let (st, _, _) = send(
        &router,
        "POST",
        "/v1/commands",
        Some(cmd(json!({"type":"deposit","idempotency_key":unique("k-bad"),"payload":{"account_no":a,"amount_cents":-5}}))),
    )
    .await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "金额非法应 400");
    let (st, _, _) = send(
        &router,
        "POST",
        "/v1/commands",
        Some(cmd(
            json!({"type":"not_a_command","idempotency_key":unique("k-unknown"),"payload":{}}),
        )),
    )
    .await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "未知命令类型应 400");
    let (st, _, _) = send(
        &router,
        "POST",
        "/v1/commands",
        Some(cmd(
            json!({"type":"reverse","idempotency_key":unique("k-m3"),"payload":{}}),
        )),
    )
    .await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "M3 未实现命令应 400");

    // 试算平衡
    let (st, _, body) = send(&router, "GET", "/v1/balance-sheet", None).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(body["balanced"], true, "借贷必须平衡: {body}");
}

/// M3 命令的 HTTP 层冒烟:冲正 / 日切 / 区间计息 + 重复提交的状态码
/// (走隔离重放库:日切/计息是全局操作,放在共享测试库会干扰其他用例)
#[tokio::test]
async fn http_冒烟_m3命令() {
    let Some(pool) = replay_pool().await else {
        eprintln!("SKIP: 缺 BANK_DB_* 环境变量");
        return;
    };
    let router = bank_api::build_router(pool);
    let acc = unique("HTTP-M3");

    let post = |body: Value| {
        let router = router.clone();
        async move {
            let (st, _, v) = send(&router, "POST", "/v1/commands", Some(body)).await;
            (st, v)
        }
    };

    // 开户 + 存款 + 冲正该笔存款(全额 → 余额回到 0)
    let (st, _) = post(cmd(
        json!({"type":"open_account","idempotency_key":unique("k"),"payload":{"account_no":acc}}),
    ))
    .await;
    assert_eq!(st, StatusCode::OK);
    let dep_key = unique("k-dep");
    let (st, _) = post(cmd(json!({"type":"deposit","idempotency_key":dep_key,"payload":{"account_no":acc,"amount_cents":5000}}))).await;
    assert_eq!(st, StatusCode::OK);
    let (st, body) = post(cmd(json!({"type":"reverse","idempotency_key":unique("k-rev"),"payload":{"target_idempotency_key":dep_key}}))).await;
    assert_eq!(st, StatusCode::OK, "冲正应成功: {body}");
    assert_eq!(body["result"]["entries"], 2, "应追加两条反向分录");
    let (st, _, accv) = send(&router, "GET", &format!("/v1/accounts/{acc}"), None).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(accv["balance_cents"], 0, "冲正存款后余额应为 0");

    // 冲正不存在的原命令 → 404
    let (st, _) = post(cmd(json!({"type":"reverse","idempotency_key":unique("k"),"payload":{"target_idempotency_key":"no-such-key"}}))).await;
    assert_eq!(st, StatusCode::NOT_FOUND);
    // 同一笔再次冲正 → 409
    let (st, body) = post(cmd(json!({"type":"reverse","idempotency_key":unique("k"),"payload":{"target_idempotency_key":dep_key}}))).await;
    assert_eq!(st, StatusCode::CONFLICT, "重复冲正应 409: {body}");

    // 唯一日期/区间,避免与其他运行撞车
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let month = 1 + (nanos % 12) as u32;
    let day = 1 + (nanos % 27) as u32;
    let d1 = format!("2099-{month:02}-{day:02}");
    let d2 = format!("2099-{month:02}-{:02}", day + 1);

    // 日切:首次 200,重复同日 409
    let (st, body) = post(cmd(
        json!({"type":"run_eod","idempotency_key":unique("k-eod"),"payload":{"day":d1}}),
    ))
    .await;
    assert_eq!(st, StatusCode::OK, "日切应成功: {body}");
    assert_eq!(body["result"]["balanced"], true);
    let (st, _) = post(cmd(
        json!({"type":"run_eod","idempotency_key":unique("k-eod"),"payload":{"day":d1}}),
    ))
    .await;
    assert_eq!(st, StatusCode::CONFLICT, "同日重复日切应 409");

    // 计息:首次 200,重复同区间 409
    let (st, body) = post(cmd(json!({"type":"settle_interest","idempotency_key":unique("k-int"),"payload":{"period_start":d1,"period_end":d2,"annual_rate_bp":360}}))).await;
    assert_eq!(st, StatusCode::OK, "计息应成功: {body}");
    let (st, _) = post(cmd(json!({"type":"settle_interest","idempotency_key":unique("k-int"),"payload":{"period_start":d1,"period_end":d2,"annual_rate_bp":360}}))).await;
    assert_eq!(st, StatusCode::CONFLICT, "同区间重复计息应 409");

    // 非法日期 → 400
    let (st, _) = post(cmd(
        json!({"type":"run_eod","idempotency_key":unique("k"),"payload":{"day":"2099-1-1"}}),
    ))
    .await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "非法日期应 400");
}
