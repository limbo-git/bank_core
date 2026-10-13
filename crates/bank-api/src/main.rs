//! bank-api 可执行入口:仅监听 127.0.0.1:8080,不暴露公网。

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    bank_api::serve().await
}
