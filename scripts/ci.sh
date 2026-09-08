#!/usr/bin/env bash
# M1 CI:格式检查 + 静态检查 + 全部测试(含真实 PG 集成测试)。
# 集成测试需要 DB:自动加载 ~/.env/database.env(BANK_DB_*),连 *_test 库。
set -euo pipefail
cd "$(dirname "$0")/.."

# 加载 DB 凭据(文件不存在则集成测试自动跳过)
if [ -f ~/.env/database.env ]; then
  set -a; source ~/.env/database.env; set +a
fi

echo "==> cargo fmt --check"
cargo fmt -- --check

echo "==> cargo clippy (deny warnings)"
cargo clippy --workspace --all-targets -- -D warnings

echo "==> cargo test --workspace"
cargo test --workspace

echo "==> CI 全绿 ✅"
