#!/usr/bin/env python3
"""把压测原始 JSONL 汇总成 markdown 报告(结论由数据算出,不写死)。"""
import json
import sys
from collections import defaultdict

path = sys.argv[1]
rows, env, prep = [], None, None
with open(path, encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if obj.get("event") == "env":
            env = obj
        elif obj.get("event") == "prepared":
            prep = obj
        elif "label" in obj:
            rows.append(obj)

by_label = {r["label"]: r for r in rows}


def tbl(rs, cols):
    head = "| " + " | ".join(c[0] for c in cols) + " |"
    sep = "|" + "|".join("---" for _ in cols) + "|"
    out = [head, sep]
    for r in rs:
        out.append("| " + " | ".join(c[1](r) for c in cols) + " |")
    return "\n".join(out)


def find(mode, sc, c, sync="on"):
    for r in rows:
        if (r["mode"], r["scenario"], r["concurrency"]) == (mode, sc, c) and r["sync_commit"] == sync:
            return r
    return None


print("# 银行核心交易系统 v1 压测报告(M4)\n")
print("> 由 `bench/run_all.sh` 自动生成;原始日志见 `bench/raw/`。\n")

print("## 1. 环境\n")
if env:
    for k in ("host", "kernel", "cpu", "mem_mb", "pg", "note"):
        if k in env:
            print(f"- **{k}**: {env[k]}")
print(f"- 每次运行每 worker 操作数: {env.get('ops_per_worker') if env else '-'}")
if prep:
    print(f"- 数据准备: {prep['accounts']} 个账户,每户 {prep['funded_cents_each']} 分(¥{prep['funded_cents_each']/100:,.2f}),"
          f"热点账户 `{prep['hot_account']}`,耗时 {prep['elapsed_ms']} ms")
print("- 口径:TPS = 成功操作数 / 墙上时间;p50/p95/p99 为单操作延迟;每轮压测结束**强制跑独立对账工具**(D1~D8)")

print("\n## 2. 引擎路径(完整命令管线:幂等 → 行锁 → 分录 → 投影)\n")
engine = [r for r in rows if r["mode"] == "engine" and r["sync_commit"] == "on"]
engine.sort(key=lambda r: (r["scenario"], r["concurrency"]))
print(tbl(engine, [
    ("场景", lambda r: r["scenario"].upper()),
    ("并发", lambda r: str(r["concurrency"])),
    ("连接池", lambda r: str(r.get("pool_max", "-"))),
    ("成功操作", lambda r: f"{r['ok']:,}"),
    ("业务拒绝", lambda r: f"{r['business_rejected']:,}"),
    ("错误", lambda r: f"{r['errors']:,}"),
    ("TPS(成功)", lambda r: f"{r['tps_ok']:,.0f}"),
    ("p50 (ms)", lambda r: f"{r['p50_ms']:.2f}"),
    ("p95 (ms)", lambda r: f"{r['p95_ms']:.2f}"),
    ("p99 (ms)", lambda r: f"{r['p99_ms']:.2f}"),
    ("对账", lambda r: "✅" if r["reconciled_clean"] else f"❌{r['reconcile_violations']}"),
]))

print("\n## 3. 裸 PG 对照(同语义:单条 CTE 语句完成账户间转账,无分录/无幂等)\n")
print("差值 = Rust 业务层(幂等领取 + 分录制 + 双投影 + 审计)的净开销。\n")
cmp_rows = []
for sc in ("s1", "s2"):
    for c in (8, 32, 128, 512):
        e, raw = find("engine", sc, c), find("raw", sc, c)
        if not e or not raw:
            continue
        cmp_rows.append({"sc": sc, "c": c, "e": e, "raw": raw})
print(tbl(cmp_rows, [
    ("场景", lambda r: r["sc"].upper()),
    ("并发", lambda r: str(r["c"])),
    ("裸 PG TPS", lambda r: f"{r['raw']['tps_ok']:,.0f}"),
    ("引擎 TPS", lambda r: f"{r['e']['tps_ok']:,.0f}"),
    ("引擎/裸 PG", lambda r: f"{r['e']['tps_ok']/max(r['raw']['tps_ok'],1e-9):.2f}x"),
    ("裸 PG p50", lambda r: f"{r['raw']['p50_ms']:.2f} ms"),
    ("引擎 p50", lambda r: f"{r['e']['p50_ms']:.2f} ms"),
    ("裸 PG 对账", lambda r: "✅" if r["raw"]["reconciled_clean"] else "❌"),
]))

print("\n## 4. fsync 成本标尺(引擎 S1 / 并发 32)\n")
on, off = find("engine", "s1", 32, "on"), find("engine", "s1", 32, "off")
if on and off:
    print(tbl([{"on": on, "off": off}], [
        ("配置", lambda r: "synchronous_commit=on(生产口径)"),
        ("TPS", lambda r: f"{r['on']['tps_ok']:,.0f}"),
        ("p50", lambda r: f"{r['on']['p50_ms']:.2f} ms"),
    ]))
    print(tbl([{"on": on, "off": off}], [
        ("配置", lambda r: "synchronous_commit=off(仅作落盘成本标尺,非生产)"),
        ("TPS", lambda r: f"{r['off']['tps_ok']:,.0f}"),
        ("p50", lambda r: f"{r['off']['p50_ms']:.2f} ms"),
    ]))
    print(f"\n**off/on 吞吐比: {off['tps_ok']/max(on['tps_ok'],1e-9):.2f}x** —— 说明 fsync 在当前硬件上的量级。")

print("\n## 5. 结论(由数据算出)\n")
n = 0
if engine:
    best = max(engine, key=lambda r: r["tps_ok"])
    n += 1
    print(f"{n}. **峰值吞吐**:{best['label']} 达 **{best['tps_ok']:,.0f} TPS**"
          f"(p99 {best['p99_ms']:.2f} ms)。")
    for sc in ("s1", "s2"):
        rs = sorted([r for r in engine if r["scenario"] == sc], key=lambda r: r["concurrency"])
        if len(rs) >= 2:
            lo, hi = rs[0], rs[-1]
            n += 1
            print(f"{n}. **{sc.upper()} 扩展性**:并发 {lo['concurrency']} → {hi['concurrency']},"
                  f"TPS {lo['tps_ok']:,.0f} → {hi['tps_ok']:,.0f}"
                  f"({hi['tps_ok']/max(lo['tps_ok'],1e-9):.2f}x),"
                  f"p99 {lo['p99_ms']:.2f} → {hi['p99_ms']:.2f} ms。")
    s1_hi = max([r for r in engine if r["scenario"] == "s1"], key=lambda r: r["concurrency"])
    s2_hi = max([r for r in engine if r["scenario"] == "s2"], key=lambda r: r["concurrency"])
    n += 1
    print(f"{n}. **热点代价**:同为最高并发,均匀(S1) {s1_hi['tps_ok']:,.0f} TPS vs "
          f"单账户热点(S2) {s2_hi['tps_ok']:,.0f} TPS —— 行锁把热点账户串行化,这就是未来"
          f"按账户分区后单个分片的上限形态。")
    ratios = [r["e"]["tps_ok"] / max(r["raw"]["tps_ok"], 1e-9) for r in cmp_rows]
    if ratios:
        n += 1
        print(f"{n}. **Rust 业务层净开销**:引擎 TPS 为裸 PG 的 {min(ratios):.2f}x ~ {max(ratios):.2f}x;"
              f"差额即幂等领取 + 分录制 + 双投影 + 审计的成本。")
    bad = [r for r in rows if not r["reconciled_clean"]]
    n += 1
    print(f"{n}. **一致性**:{len(rows)} 轮压测后全部对账 {'**零违规** ✅' if not bad else f'发现问题 {len(bad)} 轮 ❌'}。")
print("\n## 6. 局限与后续\n")
print("- 压测客户端与 PG **同机**(4 核 VM),读写争抢 CPU;绝对 TPS 仅供本机比较,横向不可外推。")
print("- 本机 `max_connections = 100`:连接池按此收敛(表中「连接池」列),因此 128/512 档是"
      "**池受限**档 —— 多余 worker 在池上排队,测的是『固定连接池下的吞吐与排队延迟』。"
      "若要看真 512 并发连接,需要提高 `max_connections` 并重启 PostgreSQL(基础设施改动,未执行)。")
print("- 裸 PG 对照只覆盖 S1/S2 的转账语义;S3 混合流无同语义基线。")
print("- 未做:连接池大小扫描、`max_connections` 与池竞争分析、更大账户规模、分布式阶段的多节点对照。")
