#!/usr/bin/env bash
# M4 压测:场景 × 并发档 × 引擎/裸 PG 对照 + fsync 标尺;每轮结束自动对账。
# 结果:bench/raw/bench-<时间戳>.jsonl(原始)+ bench/benchmark_report.md(报告)
set -euo pipefail
cd "$(dirname "$0")/.."

if [ -f ~/.env/database.env ]; then set -a; source ~/.env/database.env; set +a; fi
./scripts/bootstrap_db.sh >/dev/null

echo "==> 编译 release"
cargo build --release -p bank-bench >/dev/null

STAMP=$(date +%Y%m%d-%H%M%S)
mkdir -p bench/raw
RAW="bench/raw/bench-$STAMP.jsonl"
BIN=target/release/bank-bench
OPS="${OPS:-200}"

# 环境信息(写入原始日志,供报告引用)
cat > "$RAW" <<EOF
{"event":"env","host":"$(uname -n)","kernel":"$(uname -r)","cpu":"$(nproc) 核","mem_mb":"$(free -m | awk '/Mem:/{print $2}')","pg":"$(PGPASSWORD="$BANK_DB_PASSWORD" psql -h "$BANK_DB_HOST" -U "$BANK_DB_USER" -d "$(echo "$BANK_DB_NAME")_bench" -tAc 'select version()' | head -c 60)","note":"压测客户端与 PostgreSQL 同机,存在资源争抢","ops_per_worker":$OPS}
EOF

echo "==> 准备压测库(1000 账户 + 热点账户)"
$BIN --prepare | tee -a "$RAW"

echo "==> 引擎路径:S1/S2/S3 × 并发 8/32/128/512"
for SC in s1 s2 s3; do
  for C in 8 32 128 512; do
    $BIN --mode engine --scenario "$SC" --concurrency "$C" --ops-per-worker "$OPS" \
      --label "$SC-engine-c$C" | tee -a "$RAW"
  done
done

echo "==> 裸 PG 对照(同语义单条 CTE 语句):S1/S2 × 并发档"
for SC in s1 s2; do
  for C in 8 32 128 512; do
    $BIN --mode raw --scenario "$SC" --concurrency "$C" --ops-per-worker "$OPS" \
      --label "$SC-raw-c$C" | tee -a "$RAW"
  done
done

echo "==> fsync 成本标尺:引擎 S1 并发 32,synchronous_commit=off"
$BIN --mode engine --scenario s1 --concurrency 32 --ops-per-worker "$OPS" \
  --sync-commit off --label "s1-engine-c32-syncoff" | tee -a "$RAW"

echo "==> 生成报告"
python3 bench/make_report.py "$RAW" > bench/benchmark_report.md
echo "原始日志: $RAW"
echo "报告: bench/benchmark_report.md"
