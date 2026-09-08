# 银行核心交易系统(Rust + PostgreSQL)项目方案

> 版本:v1.0(2026-09-08,经 grill 访谈逐项定案)
> 定位:**仿真生产级**——按生产标准写正确性,不接真实资金
> 终局:先单机压测验证 → 按用户设想的**事件驱动 + 加机器扩吞吐**(可接受更高单笔延迟)改造;数据库(PG)始终是"记账的一部分"

---

## 1. 已定决策总表(grill 结论)

| 维度 | 决策 |
|---|---|
| 动机(Q1) | 仿真核心,不碰真钱;技术学习 + 自研路线性能验证,持续投入 |
| 业务范围(Q2) | 账户/科目 + 双分录总账 + 存取/转账/冲正 + 计息 + 日切 + 试算平衡/对账 |
| 真实度(Q3) | 仿真生产级:ACID、幂等、审计、流水不可变 |
| 验收形态(Q4) | REST API + 自动化校验/对账 + 压测报告归档(摘要发聊天,明细落文件) |
| 资源(Q5) | 持续投入,单人,本机 VM,先单机后分布式 |
| 分布式愿景(Q6) | 事件驱动;加机器扩吞吐;PG 始终是记账的一部分 |
| 单机架构(Q7) | Rust 业务层 + PostgreSQL 管事务/持久化(PG 即账本) |
| API 形态(Q8) | v1 REST(axum),分布式阶段内部加 gRPC(接受双栈) |
| 账务真相源(Q9) | 分录流水不可变追加 = 真相源;余额字段 = 同事务维护的投影 |
| 并发控制(Q10) | 账户行锁 `SELECT ... FOR UPDATE`;锁粒度 = 未来分区键 |
| 幂等(Q11) | 全命令幂等键 + PG 唯一约束;重复提交返回原结果 |
| 计息/日切(Q12) | 区间计息(日终余额 × 利率 ÷ 360,结算日一次入账);轻量日切 = 可重放快照 + 试算平衡 |
| 账户/命令面(Q13) | 单币种 CNY、单类型活期(字段预留);命令集见 §5;金额 i64 分 + Money newtype,零浮点 |
| 记账深度(Q14) | 客户账户 + 精简科目树(资产/负债/收入/费用);试算平衡为真校验 |
| 命令账(Q15) | 命令表三合一:幂等锚点 + 审计线索 + 未来事件源;状态机含"已提交但结果未知"中间态 |
| 压测(Q16/17) | S1 均匀 / S2 热点 / S3 混合 × 并发 8/32/128/512;TPS + p50/p95/p99 + 失败率;加裸 PG 对照基线 |
| 预演测试(Q18) | 确定性重放(同命令流重放两遍状态一致)+ kill -9 恢复测试,单机阶段就做 |
| 落地(Q19/20) | 里程碑制(M0~M4),每段交付"可运行代码 + 测试全绿 + 产物归档",逐段验收 |
| 收尾(Q21) | 目录 `~/bank_core`;axum + sqlx + tokio + serde + proptest;服务与 PG 均绑 127.0.0.1;凭据 `~/.env/database.env`(600);文档/注释/测试用例中文;计息单利 ÷360;冲正仅全额、每笔原交易限一次 |

---

## 2. 总体架构

```
┌──────────────────────────── 单机 v1 ────────────────────────────┐
│                                                                 │
│  客户端 / 压测工具 ──REST──> bank-api (axum, 127.0.0.1:8080)      │
│                                  │ POST /v1/commands             │
│                                  ▼                              │
│   ┌─────────────────────────────────────────────────┐           │
│   │ 命令处理管线(每命令一个 PG 事务)                  │           │
│   │  1. 幂等检查(幂等键唯一约束)                      │           │
│   │  2. 账户行锁 FOR UPDATE(按 account_no 排序加锁)   │           │
│   │  3. 领域校验(余额/状态/金额 > 0)                  │           │
│   │  4. 写命令账 + 追加分录 + 更新余额投影(同事务)     │           │
│   │  5. commit(unique_violation → 重查返回原结果)     │           │
│   └─────────────────────────────────────────────────┘           │
│                        │                                         │
│                        ▼                                         │
│   PostgreSQL 18.6 (127.0.0.1:5432, 记账本体)                    │
│   命令账 / 分录流水(不可变)/ 账户投影 / 科目 / 日终快照          │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
          │ 数据形态即事件源:命令账 + 不可变分录
          ▼ (未来:分区 + 订阅 + 投影横向扩展 = 分布式改造)
```

**分层原则**:领域层零 IO、零时钟(时钟注入);事务脚本收敛在 store 层;API 层只做协议转换。这条边界是未来分布式中"确定性内核"能原样搬走的前提。

---

## 3. 领域模型(核心概念)

- **金额**:内部一律 `i64` 最小单位(分),包装为 `Money` newtype(禁止与裸 i64/浮点混用)。所有金额校验 `> 0`,方向由借贷表达。
- **命令(Command)**:外部请求的唯一入口形态,必带幂等键。类型:`open_account / deposit / withdraw / transfer / reverse / settle_interest / run_eod`。
- **命令状态机**:`received → committed`,或 `received → failed`;网络/进程故障时落 **`outcome_unknown`(已提交但结果未知)**——客户端带同幂等键重查即可收敛,这是未来分区重试语义的同一段代码。
- **分录(Entry)**:不可变,借贷平衡组(Σdebit = Σcredit > 0)。冲正 = 追加一组反向分录并引用原命令,绝不 UPDATE/DELETE 原流水。
- **记账位**:客户账户(科目 2001"活期存款"的明细)或银行内部科目(现金/手续费/利息费用)。每行分录冗余 `coa_id`,科目汇总与试算平衡不依赖明细表。
- **投影**:`account.balance_cents`(客户可用余额)、`coa_balance.balance_cents`(科目余额)。只读查询走投影;对账工具随时可从分录重算交叉验证。

**记账示例**(口径:客户存款是银行负债,客户视角正数):
| 交易 | 分录 |
|---|---|
| 存入 100 | 借 1001 现金 100 / 贷 2001 客户A 存款 100 |
| 取款 30 | 借 2001 客户A 存款 30 / 贷 1001 现金 30 |
| A→B 转账 50 | 借 2001 客户A 50 / 贷 2001 客户B 50(两客户负债对冲) |
| 计息入账 1.23 | 借 6601 利息费用 1.23 / 贷 2001 客户A 1.23 |

---

## 4. 数据模型(PG DDL 草案)

```sql
-- 科目表(精简树,期初预置:1001 现金[资产] / 2001 活期存款[负债] / 6001 手续费收入[收入] / 6601 利息费用[费用])
CREATE TABLE coa (
  id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  code        text NOT NULL UNIQUE,
  name        text NOT NULL,
  category    text NOT NULL CHECK (category IN ('asset','liability','equity','income','expense')),
  normal_side text NOT NULL CHECK (normal_side IN ('debit','credit'))
);

-- 客户账户(负债类科目 2001 的明细账)
CREATE TABLE account (
  id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  account_no    text NOT NULL UNIQUE,
  holder        text,
  type          text NOT NULL DEFAULT 'current',   -- 预留:多类型
  currency      char(3) NOT NULL DEFAULT 'CNY',    -- 预留:多币种
  coa_id        bigint NOT NULL REFERENCES coa(id),
  status        text NOT NULL DEFAULT 'active' CHECK (status IN ('active','frozen','closed')),
  balance_cents bigint NOT NULL DEFAULT 0 CHECK (balance_cents >= 0),  -- 投影
  opened_at     timestamptz NOT NULL DEFAULT now()
);

-- 银行内部科目余额投影(现金/手续费/利息费用)
CREATE TABLE coa_balance (
  coa_id        bigint PRIMARY KEY REFERENCES coa(id),
  balance_cents bigint NOT NULL DEFAULT 0
);

-- 命令账:幂等锚点 + 审计线索 + 未来事件源(三合一)
CREATE TABLE command_log (
  id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  idempotency_key text NOT NULL UNIQUE,            -- 幂等硬约束
  command_type    text NOT NULL,
  payload         jsonb NOT NULL,
  status          text NOT NULL DEFAULT 'received'
                  CHECK (status IN ('received','committed','outcome_unknown','failed')),
  created_at      timestamptz NOT NULL DEFAULT now(),
  committed_at    timestamptz
);

-- 分录流水:真相源,只追加
CREATE TABLE ledger_entry (
  id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  command_id     bigint NOT NULL REFERENCES command_log(id),
  seq            smallint NOT NULL,                -- 命令内序号
  coa_id         bigint NOT NULL REFERENCES coa(id),   -- 记账科目
  account_id     bigint REFERENCES account(id),    -- 客户账户记账位(银行内部科目为 NULL)
  side           text NOT NULL CHECK (side IN ('debit','credit')),
  amount_cents   bigint NOT NULL CHECK (amount_cents > 0),
  reverses_command_id bigint REFERENCES command_log(id),  -- 冲正引用
  booked_at      timestamptz NOT NULL,
  UNIQUE (command_id, seq),
  CHECK (account_id IS NOT NULL OR coa_id <> 2001)  -- 银行内部科目不得挂客户明细(示意,见注)
);
-- 不可变铁律:REVOKE UPDATE, DELETE ON ledger_entry FROM ... + 触发器拒改

-- 日终快照(轻量日切:可重放 + 试算平衡)
CREATE TABLE eod_snapshot (
  day        date PRIMARY KEY,
  closed_at  timestamptz NOT NULL,
  balanced   boolean NOT NULL                      -- 试算平衡结果
);
CREATE TABLE account_daily_balance (               -- 供区间计息
  account_id    bigint NOT NULL REFERENCES account(id),
  day           date NOT NULL,
  balance_cents bigint NOT NULL,
  PRIMARY KEY (account_id, day)
);
```

> 注:分录的 `coa_id` 冗余字段让科目试算无需 join 明细;`account_id` 为空即银行内部记账位。约束细节(如"2001 明细必须挂 account_id")在 M1 迁移里以正式 CHECK/触发器收紧,并以对账工具兜底复核。

---

## 5. 命令面(REST,统一入口)

**统一命令入口**(贴合"命令 = 未来事件"的形态,REST 只是过渡 transport):

| 方法/路径 | 语义 |
|---|---|
| `POST /v1/commands` | body `{idempotency_key, type, payload}`;重复键返回原结果 + `X-Replay: true` |
| `GET /v1/commands/{idempotency_key}` | 查命令状态(含 outcome_unknown 收敛) |
| `GET /v1/accounts/{account_no}` | 余额/状态 |
| `GET /v1/accounts/{account_no}/entries` | 账户流水(分页) |
| `GET /v1/balance-sheet` | 科目试算(供对账工具/人工) |

**命令 payload 要点**:
- `deposit/withdraw/transfer`:账户号、金额;transfer 双账户按 `account_no` 字典序加锁(防死锁)
- `reverse`:被冲原命令的幂等键(仅全额;每笔原交易限冲一次,靠 `reverses_command_id` 唯一性约束)
- `settle_interest`:区间 + 利率档(默认年利率 ÷ 360 单利,结算日一次入账)
- `run_eod`:按注入时钟的"当日"生成快照 + 试算平衡(可重放:同一天重跑结果一致)

---

## 6. 测试矩阵(为分布式预演写准,全部中文命名)

**T-A 单元 / 属性测试(bank-domain,无 IO)**
- T-A1:Money 运算(加减、无溢出、无精度损失),proptest 随机金额
- T-A2:命令校验规则(金额 > 0、账户存在、状态 active、冲正引用合法)
- T-A3:分录组借贷平衡性质(proptest:任意组合 Σdebit = Σcredit)
- T-A4:冲正规则(全额、一次、原流水不可变)

**T-B 集成测试(真实 PG,单命令)**
- T-B1:开户→存取→转账→查余额/流水,全 happy path
- T-B2:每个命令的失败路径(余额不足、账户冻结、金额非法、冲正越权)
- T-B3:幂等——同键重发(顺序重发、**并发同键**)返回原结果,账只记一次
- T-B4:冲正后余额/科目恢复,原分录原样保留

**T-C 并发正确性(核心)**
- T-C1:同付款账户并发转账 N 笔(如 1000 笔 × 并发 32)→ 无超扣、余额守恒
- T-C2:双账户互转(AB/BA 并发)→ 无死锁超时后全部成功或安全失败,无超扣
- T-C3:并发冲正与转账竞争同一账户 → 一致性
- T-C4:每轮并发风暴后跑对账工具,零差异

**T-D 对账工具(独立仲裁者,与引擎代码分离)**
- T-D1:Σ分录(按科目方向)= 科目投影;Σ分录(客户维度)= 账户投影
- T-D2:全库借贷平衡;幂等键无重复;分录无 UPDATE/DELETE(可加审计触发器检测)
- T-D3:**错误注入演练**:人为篡改一个投影值,对账工具必须抓出

**T-E 分布式预演(单机就做)**
- T-E1:**确定性重放**:导出命令流 → 空库重放两遍 → 账户余额、科目余额、快照逐字段一致(黄金测试)
- T-E2:**kill -9 故障**:转账中途杀服务 / 杀 PG → 重启后命令收敛(无半笔、无超扣、outcome_unknown 可重查),幂等键重发收敛
- T-E3:时钟注入:同命令流在不同"日切时刻"重放,日终/计息结果确定

**T-F 压测**(§7,产物归档)

---

## 7. 压测设计

**场景 × 并发**(每场景 8/32/128/512 四档,预热后计时,每档 3 次取中位数——沿用既往实验口径):
- **S1 均匀随机转账**:N 账户对均匀随机(如 10 万账户,测吞吐上限与平局延迟)
- **S2 单账户热点风暴**:全部转账打向同一收款/付款账户(测行锁串行化、队头延迟 = 未来单分片最坏情形)
- **S3 混合命令流**:转账 70% + 存取 15% + 冲正 10% + 查询 5%(贴近真实,含读)

**指标**:TPS、p50/p95/p99 延迟、失败率。**对照基线**:同语义裸 PG(单条 SQL 事务)同负载跑一遍,差值 = Rust 层净开销。

**环境注意**:压测客户端与 PG 同机(4C VM),资源争抢如实记录;对比裸 PG 基线可剥离一部分。fsync 成本用 `synchronous_commit=on/off` 两档量化(off 档仅作落盘成本标尺,明确非生产配置)。

**执行与归档**:预置账户脚本 → 场景脚本 → 采集 → **每轮压测后强制跑对账工具**(压测再猛,账必须平)→ 归档 `bench/benchmark_report.md` + `bench/raw/` 原始日志 + 可复跑脚本;摘要发聊天。

---

## 8. 工程结构(workspace)

```
~/bank_core/
├── PROJECT_PLAN.md          # 本方案
├── Cargo.toml               # workspace
├── crates/
│   ├── bank-domain/         # 纯 Rust:Money、命令/分录/科目领域类型与规则,零 IO、时钟注入
│   ├── bank-store/          # sqlx:迁移、仓储、事务脚本(幂等提交/行锁/分录追加/投影更新)
│   └── bank-api/            # axum:REST 命令入口与查询
├── migrations/              # sqlx migrate 版本化 DDL
├── tests/                   # T-B~T-E 集成/并发/重放/kill 测试
├── tools/
│   ├── reconciler/          # 对账工具(独立二进制,故意不依赖引擎内部 API)
│   └── bench/               # 压测客户端(自写,tokio,支持 S1/S2/S3 与裸 PG 对照)
└── bench/                   # 压测归档(report/raw logs/复跑脚本)
```

> 依赖走 rsproxy 镜像(已配);避免 git 依赖(github 不稳定)。crate 边界 = 未来分布式的物理拆分线。

---

## 9. 里程碑与验收(M0~M4,逐段放行)

| 阶段 | 内容 | 验收标准 |
|---|---|---|
| **M0 工程骨架** | workspace、Money/领域类型、错误模型、sqlx 迁移、CI 脚本 | 单测绿;迁移空库可重跑;`cargo test` 一键过 |
| **M1 数据模型+命令面** | 全表 DDL、开户/存取/转账/查询、幂等键、REST 入口 | T-B 全绿;curl 转账可查余额;幂等重发不重账 |
| **M2 并发正确性+对账** | 行锁语义、死锁规避、对账工具、T-C/T-D | 并发风暴无超扣;错误注入被抓出 |
| **M3 冲正/计息/日切+预演** | reverse、settle_interest、run_eod、T-E1~E3 | 重放两遍状态一致;kill 后无半笔 |
| **M4 压测与报告** | S1~S3 × 并发档 + 裸 PG 对照 + 归档 | `bench/benchmark_report.md` 产出,摘要发聊天 |

每段交付:**可运行代码 + 测试全绿 + 产物归档**,你验收后放行下一段。压测大纲(§7)从 M1 起即为数据模型/测试的硬约束。

---

## 10. 风险与对策

| 风险 | 对策 |
|---|---|
| 热点账户行锁队头延迟 | S2 专门量化;若不可接受,备选(乐观重试/批处理)等压测数据说话,不预实现 |
| 转账双锁死锁 | 按 account_no 字典序加锁 + 有限重试;T-C2 覆盖 |
| PG fsync 是吞吐天花板 | synchronous_commit 两档量化,结论标注仿真口径 |
| 并发同幂等键 | 唯一约束 + unique_violation 捕获后重查原结果;T-B3 并发用例 |
| 重放不确定性(now()/随机混入) | 领域层禁 now()/random,时钟注入;T-E1/T-E3 把关 |
| 双写不一致(分录 vs 投影) | 同事务内完成,无独立双写;对账工具独立复核(T-D) |
| 压测机与 PG 同机争抢 | 环境如实记录;裸 PG 对照剥离系统开销 |
| 幂等键/命令账膨胀 | 仿真规模无虞;如需要加归档策略,分布式阶段再议 |
| 范围蔓延(计息/日切细节) | 口径已在 §1/Q21 钉死;超出口径的一律 v2 |

---

## 11. 环境与安全约定

- PostgreSQL 18.6(现成,仅 127.0.0.1:5432);新建独立库(如 `bank_core`)+ 独立用户,凭据入 `~/.env/database.env`(chmod 600)
- bank-api 监听 127.0.0.1:8080,不暴露公网
- 所有产物(测试表、脚本、日志、报告)保留不清理(沿用惯例)
