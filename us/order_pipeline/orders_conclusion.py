import sys

import time

from ..utils.helpers import countdown

from ..config.db_conn import engine

from sqlalchemy import text, Table, MetaData

# =========================================
# 多週期資料整合（以 D 為主時間軸）
# 一列 = ticker + Date_D
# 其他週期用 as-of join 對齊
# =========================================

def upsert_stock_side(engine,
                    schema_name: str = "US",
                    table_name: str = None):
    """

    必要欄位：
        "Date_D" ... 
        "ticker", ... 
        "Side_1_D", ...
    """

    # ---------------------------
    # 1.建表(第一次用)

    create_sql = f"""
        CREATE TABLE IF NOT EXISTS "{schema_name}"."{table_name}" (
            "Date_D" DATE NOT NULL,
            "ticker" VARCHAR(20) NOT NULL,
            "Date_W" DATE NOT NULL,
            "Date_2W" DATE NOT NULL,
            "Date_3W" DATE NOT NULL,
            "Date_ME" DATE NOT NULL,
            "Date_2M" DATE NOT NULL,
            "Date_3M" DATE NOT NULL,
            PRIMARY KEY ("Date_D", "ticker")
    );
    """
    # ---------------------------
    # 2.更新表欄位

    alter_sql =f"""

        ALTER TABLE "{schema_name}"."{table_name}"
            ADD COLUMN IF NOT EXISTS "Side_1_D"  TEXT,
            ADD COLUMN IF NOT EXISTS "Side_1_W"  TEXT,
            ADD COLUMN IF NOT EXISTS "Side_1_2W" TEXT,
            ADD COLUMN IF NOT EXISTS "Side_1_3W" TEXT,
            ADD COLUMN IF NOT EXISTS "Side_1_ME" TEXT,
            ADD COLUMN IF NOT EXISTS "Side_1_2M" TEXT,
            ADD COLUMN IF NOT EXISTS "Side_1_3M" TEXT,

            ADD COLUMN IF NOT EXISTS "Side_2_D"  TEXT,
            ADD COLUMN IF NOT EXISTS "Side_2_W"  TEXT,
            ADD COLUMN IF NOT EXISTS "Side_2_2W" TEXT,
            ADD COLUMN IF NOT EXISTS "Side_2_3W" TEXT,
            ADD COLUMN IF NOT EXISTS "Side_2_ME" TEXT,
            ADD COLUMN IF NOT EXISTS "Side_2_2M" TEXT,
            ADD COLUMN IF NOT EXISTS "Side_2_3M" TEXT,

            ADD COLUMN IF NOT EXISTS "Side_3_D"  TEXT,
            ADD COLUMN IF NOT EXISTS "Side_3_W"  TEXT,
            ADD COLUMN IF NOT EXISTS "Side_3_2W" TEXT,
            ADD COLUMN IF NOT EXISTS "Side_3_3W" TEXT,
            ADD COLUMN IF NOT EXISTS "Side_3_ME" TEXT,
            ADD COLUMN IF NOT EXISTS "Side_3_2M" TEXT,
            ADD COLUMN IF NOT EXISTS "Side_3_3M" TEXT;
    """


    # ---------------------------
    # 3.更新表資料

    upsert_sql = f"""
        INSERT INTO "{schema_name}"."{table_name}" (
            "ticker",
            "Date_D", "Side_1_D", "Side_2_D", "Side_3_D",
            "Date_3M", "Side_1_3M", "Side_2_3M", "Side_3_3M",
            "Date_2M", "Side_1_2M", "Side_2_2M", "Side_3_2M",
            "Date_ME", "Side_1_ME", "Side_2_ME", "Side_3_ME",
            "Date_3W", "Side_1_3W", "Side_2_3W", "Side_3_3W",
            "Date_2W", "Side_1_2W", "Side_2_2W", "Side_3_2W",
            "Date_W",  "Side_1_W",  "Side_2_W",  "Side_3_W"
        )

        WITH base_table AS (
            SELECT
                "ticker",
                "Date" AS "Date_D",
                "Side_1" AS "Side_1_D",
                "Side_2" AS "Side_2_D",
                "Side_3" AS "Side_3_D"
            FROM "US"."prices_engine_D"
            WHERE "Date" >= CURRENT_DATE - INTERVAL '2 years'
        )

        SELECT 
            -- D
            b."ticker",
            b."Date_D",
            b."Side_1_D",
            b."Side_2_D",
            b."Side_3_D",

            -- 3M
            m3."Date"  AS "Date_3M",
            m3."Side_1" AS "Side_1_3M",
            m3."Side_2" AS "Side_2_3M",
            m3."Side_3" AS "Side_3_3M",

            -- 2M
            m2."Date"  AS "Date_2M",
            m2."Side_1" AS "Side_1_2M",
            m2."Side_2" AS "Side_2_2M",
            m2."Side_3" AS "Side_3_2M",

            -- ME
            me."Date"  AS "Date_ME",
            me."Side_1" AS "Side_1_ME",
            me."Side_2" AS "Side_2_ME",
            me."Side_3" AS "Side_3_ME",

            -- 3W
            w3."Date"  AS "Date_3W",
            w3."Side_1" AS "Side_1_3W",
            w3."Side_2" AS "Side_2_3W",
            w3."Side_3" AS "Side_3_3W",

            -- 2W
            w2."Date"  AS "Date_2W",
            w2."Side_1" AS "Side_1_2W",
            w2."Side_2" AS "Side_2_2W",
            w2."Side_3" AS "Side_3_2W",

            -- W
            w."Date"  AS "Date_W",
            w."Side_1" AS "Side_1_W",
            w."Side_2" AS "Side_2_W",
            w."Side_3" AS "Side_3_W"

        FROM base_table b

        LEFT JOIN LATERAL (
            SELECT "Date", "Side_1", "Side_2", "Side_3"
            FROM "US"."prices_engine_3M"
            WHERE "ticker" = b."ticker"
            AND "Date" <= b."Date_D" + INTERVAL '93 days'
            ORDER BY "Date" DESC
            LIMIT 1
        ) m3 ON TRUE

        LEFT JOIN LATERAL (
            SELECT "Date", "Side_1", "Side_2", "Side_3"
            FROM "US"."prices_engine_2M"
            WHERE "ticker" = b."ticker"
            AND "Date" <= b."Date_D" + INTERVAL '63 days'
            ORDER BY "Date" DESC
            LIMIT 1
        ) m2 ON TRUE

        LEFT JOIN LATERAL (
            SELECT "Date", "Side_1", "Side_2", "Side_3"
            FROM "US"."prices_engine_ME"
            WHERE "ticker" = b."ticker"
            AND "Date" <= b."Date_D" + INTERVAL '32 days'
            ORDER BY "Date" DESC
            LIMIT 1
        ) me ON TRUE

        LEFT JOIN LATERAL (
            SELECT "Date", "Side_1", "Side_2", "Side_3"
            FROM "US"."prices_engine_3W"
            WHERE "ticker" = b."ticker"
            AND "Date" <= b."Date_D" + INTERVAL '22 days'
            ORDER BY "Date" DESC
            LIMIT 1
        ) w3 ON TRUE

        LEFT JOIN LATERAL (
            SELECT "Date", "Side_1", "Side_2", "Side_3"
            FROM "US"."prices_engine_2W"
            WHERE "ticker" = b."ticker"
            AND "Date" <= b."Date_D" + INTERVAL '15 days'
            ORDER BY "Date" DESC
            LIMIT 1
        ) w2 ON TRUE

        LEFT JOIN LATERAL (
            SELECT "Date", "Side_1", "Side_2", "Side_3"
            FROM "US"."prices_engine_W"
            WHERE "ticker" = b."ticker"
            AND "Date" <= b."Date_D" + INTERVAL '8 days'
            ORDER BY "Date" DESC
            LIMIT 1
        ) w ON TRUE

        ON CONFLICT ("Date_D", "ticker")
        DO NOTHING;

    """

    with engine.begin() as conn:

        conn.execute(text(create_sql))
        conn.execute(text(alter_sql))
        conn.execute(text(upsert_sql))

def main():

    try:

        upsert_stock_side(engine = engine,
                        schema_name = "US",
                        table_name = "orders_conclusion")
        time.sleep(1)

    except Exception as e:
        print("發生錯誤 !", e)

    return

if __name__ == "__main__":
    main()
    countdown(10)

# 強制關閉程序
sys.exit()