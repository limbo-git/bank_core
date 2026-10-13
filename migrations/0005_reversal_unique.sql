-- 0005: 冲正唯一性的正确落点 —— 命令级而非分录级
--
-- 背景:一次冲正会追加多条反向分录,它们共享同一个 reverses_command_id,
-- 因此"每条原交易只能被冲正一次"的约束必须落在**命令**上,而不是分录行上。
-- 0004 里建在 ledger_entry(reverses_command_id) 的唯一索引会导致一次冲正就自撞, 现予废除。

DROP INDEX IF EXISTS idx_ledger_entry_one_reversal;

-- 命令级冲正登记:一个原命令最多被一个冲正命令引用
ALTER TABLE command_log
    ADD COLUMN reverses_command_id bigint REFERENCES command_log(id);

CREATE UNIQUE INDEX idx_command_log_one_reversal
    ON command_log (reverses_command_id)
    WHERE reverses_command_id IS NOT NULL;

COMMENT ON COLUMN command_log.reverses_command_id IS '本命令冲正的原命令;唯一索引保证每笔原交易最多被冲正一次';
