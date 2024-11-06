from fiber.logging_utils import get_logger

from asyncpg import Connection
import random

from validator.db.src.database import PSQLDB
from validator.models import Contender, PeriodScore, calculate_period_score
from validator.utils.database import database_constants as dcst
from validator.utils.generic import generic_constants as gcst
from validator.utils.database import database_constants as dcst

logger = get_logger(__name__)


async def insert_contenders(connection: Connection, contenders: list[Contender], validator_hotkey: str) -> None:
    logger.debug(f"Inserting {len(contenders)} contender records")

    await connection.executemany(
        f"""
        INSERT INTO {dcst.CONTENDERS_TABLE} (
            {dcst.CONTENDER_ID},
            {dcst.NODE_HOTKEY},
            {dcst.NODE_ID},
            {dcst.NETUID},
            {dcst.TASK},
            {dcst.VALIDATOR_HOTKEY},
            {dcst.CAPACITY},
            {dcst.RAW_CAPACITY},
            {dcst.CAPACITY_TO_SCORE},
            {dcst.CONSUMED_CAPACITY},
            {dcst.TOTAL_REQUESTS_MADE},
            {dcst.REQUESTS_429},
            {dcst.REQUESTS_500},
            {dcst.CREATED_AT},
            {dcst.UPDATED_AT}
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, NOW(), NOW())
        """,
        [
            (
                contender.id,
                contender.node_hotkey,
                contender.node_id,
                contender.netuid,
                contender.task,
                validator_hotkey,
                contender.capacity,
                contender.raw_capacity,
                contender.capacity_to_score,
                contender.consumed_capacity,
                contender.total_requests_made,
                contender.requests_429,
                contender.requests_500,
            )
            for contender in contenders
        ],
    )


async def migrate_contenders_to_contender_history(connection: Connection) -> None:
    await connection.execute(
        f"""
        INSERT INTO {dcst.CONTENDERS_HISTORY_TABLE} (
            {dcst.CONTENDER_ID},
            {dcst.NODE_HOTKEY},
            {dcst.NODE_ID},
            {dcst.NETUID},
            {dcst.TASK},
            {dcst.VALIDATOR_HOTKEY},
            {dcst.CAPACITY},
            {dcst.RAW_CAPACITY},
            {dcst.CAPACITY_TO_SCORE},
            {dcst.CONSUMED_CAPACITY},
            {dcst.TOTAL_REQUESTS_MADE},
            {dcst.REQUESTS_429},
            {dcst.REQUESTS_500},
            {dcst.PERIOD_SCORE},
            {dcst.CREATED_AT},
            {dcst.UPDATED_AT}
        )
        SELECT
            {dcst.CONTENDER_ID},
            {dcst.NODE_HOTKEY},
            {dcst.NODE_ID},
            {dcst.NETUID},
            {dcst.TASK},
            {dcst.VALIDATOR_HOTKEY},
            {dcst.CAPACITY},
            {dcst.RAW_CAPACITY},
            {dcst.CAPACITY_TO_SCORE},
            {dcst.CONSUMED_CAPACITY},
            {dcst.TOTAL_REQUESTS_MADE},
            {dcst.REQUESTS_429},
            {dcst.REQUESTS_500},
            {dcst.PERIOD_SCORE},
            {dcst.CREATED_AT},
            {dcst.UPDATED_AT}
        FROM {dcst.CONTENDERS_TABLE}
        """
    )

    await connection.execute(f"DELETE FROM {dcst.CONTENDERS_TABLE}")


async def get_contenders_for_synthetic_task(connection: Connection, task: str, top_x: int = 5 )-> list[Contender]:
    rows = await connection.fetch(
        f"""
        WITH ranked_contenders AS (
            SELECT 
                c.{dcst.CONTENDER_ID}, c.{dcst.NODE_HOTKEY}, c.{dcst.NODE_ID}, c.{dcst.TASK},
                c.{dcst.RAW_CAPACITY}, c.{dcst.CAPACITY_TO_SCORE}, c.{dcst.CONSUMED_CAPACITY},
                c.{dcst.TOTAL_REQUESTS_MADE}, c.{dcst.REQUESTS_429}, c.{dcst.REQUESTS_500}, 
                c.{dcst.CAPACITY}, c.{dcst.PERIOD_SCORE}, c.{dcst.NETUID},
                ROW_NUMBER() OVER (
                    ORDER BY c.{dcst.TOTAL_REQUESTS_MADE} ASC
                ) AS rank
            FROM {dcst.CONTENDERS_TABLE} c
            JOIN {dcst.NODES_TABLE} n ON c.{dcst.NODE_ID} = n.{dcst.NODE_ID} AND c.{dcst.NETUID} = n.{dcst.NETUID}
            WHERE c.{dcst.TASK} = $1 
            AND c.{dcst.CAPACITY} > 0 
            AND n.{dcst.SYMMETRIC_KEY_UUID} IS NOT NULL
        )
        SELECT *
        FROM ranked_contenders
        WHERE rank <= $2
        ORDER BY rank
        """,
        task,
        top_x,
    )

    # If not enough rows are returned, run another query to get more contenders
    if not rows or len(rows) < top_x:
        additional_rows = await connection.fetch(
            f"""
            SELECT 
                c.{dcst.CONTENDER_ID}, c.{dcst.NODE_HOTKEY}, c.{dcst.NODE_ID}, c.{dcst.TASK},
                c.{dcst.RAW_CAPACITY}, c.{dcst.CAPACITY_TO_SCORE}, c.{dcst.CONSUMED_CAPACITY},
                c.{dcst.TOTAL_REQUESTS_MADE}, c.{dcst.REQUESTS_429}, c.{dcst.REQUESTS_500}, 
                c.{dcst.CAPACITY}, c.{dcst.PERIOD_SCORE}, c.{dcst.NETUID}
            FROM {dcst.CONTENDERS_TABLE} c
            JOIN {dcst.NODES_TABLE} n ON c.{dcst.NODE_ID} = n.{dcst.NODE_ID} AND c.{dcst.NETUID} = n.{dcst.NETUID}
            WHERE c.{dcst.TASK} = $1 
            AND c.{dcst.CAPACITY} > 0 
            AND n.{dcst.SYMMETRIC_KEY_UUID} IS NOT NULL
            ORDER BY c.{dcst.TOTAL_REQUESTS_MADE} ASC
            LIMIT $2
            OFFSET $3
            """,
            task,
            top_x - len(rows) if rows else top_x,
            len(rows) if rows else 0,
        )
        rows = rows + additional_rows if rows else additional_rows

    return [Contender(**row) for row in rows]
    

async def get_contenders_for_organic_task(connection: Connection, task: str, top_x: int = 5) -> list[Contender]:
    """
    Select contenders using round-robin with dedicated tracking fields.
    Uses new columns for better load distribution metrics.
    """
    rows = await connection.fetch(
        f"""
        WITH viable_contenders AS (
            SELECT 
                c.*,
                -- Calculate load and health metrics
                CASE 
                    WHEN c.{dcst.TOTAL_REQUESTS_MADE} > 0 THEN 
                        (c.{dcst.REQUESTS_429} + c.{dcst.REQUESTS_500})::float / 
                        NULLIF(c.{dcst.TOTAL_REQUESTS_MADE}, 0)
                    ELSE 0
                END as error_rate
            FROM {dcst.CONTENDERS_TABLE} c
            JOIN {dcst.NODES_TABLE} n ON c.{dcst.NODE_ID} = n.{dcst.NODE_ID} 
                AND c.{dcst.NETUID} = n.{dcst.NETUID}
            WHERE c.{dcst.TASK} = $1
            AND c.{dcst.CAPACITY} > 0
            AND n.{dcst.SYMMETRIC_KEY_UUID} IS NOT NULL
        ),
        healthy_contenders AS (
            SELECT *
            FROM viable_contenders
            WHERE error_rate < 0.3
            AND {dcst.CAPACITY} - {dcst.CONSUMED_CAPACITY} > 0
            -- Skip if timeout recently
            AND (last_timeout_at IS NULL OR NOW() - last_timeout_at > interval '10 seconds')
            -- Skip if too many recent timeouts
            AND timeouts_last_minute < 10
            -- Skip if too many recent queries
            AND organic_queries_last_minute < 30
        ),
        ranked_contenders AS (
            SELECT 
                *,
                ROW_NUMBER() OVER (
                    ORDER BY 
                        -- First by last query time
                        last_organic_query_at ASC NULLS FIRST,
                        -- Then by recent load
                        organic_queries_last_minute ASC,
                        -- Then by recent timeouts
                        timeouts_last_minute ASC
                ) as usage_rank
            FROM healthy_contenders
        )
        SELECT * 
        FROM ranked_contenders
        WHERE usage_rank <= $2
        ORDER BY usage_rank
        """,
        task,
        top_x
    )

    if not rows or len(rows) < top_x:
        logger.debug(f"Not enough viable contenders ({len(rows) if rows else 0} < {top_x}), falling back to synthetic")
        return await get_contenders_for_synthetic_task(connection, task, top_x)

    # Convert rows to contenders, maintaining order
    contenders = [
        Contender(
            id=row[dcst.CONTENDER_ID],
            node_hotkey=row[dcst.NODE_HOTKEY],
            node_id=row[dcst.NODE_ID],
            task=row[dcst.TASK],
            raw_capacity=row[dcst.RAW_CAPACITY],
            capacity_to_score=row[dcst.CAPACITY_TO_SCORE],
            consumed_capacity=row[dcst.CONSUMED_CAPACITY],
            total_requests_made=row[dcst.TOTAL_REQUESTS_MADE],
            requests_429=row[dcst.REQUESTS_429],
            requests_500=row[dcst.REQUESTS_500],
            capacity=row[dcst.CAPACITY],
            period_score=row[dcst.PERIOD_SCORE],
            netuid=row[dcst.NETUID]
        )
        for row in rows[:top_x]
    ]

    # Update tracking fields for selected contenders
    if contenders:
        await connection.execute(
            f"""
            UPDATE {dcst.CONTENDERS_TABLE}
            SET last_organic_query_at = NOW(),
                organic_queries_last_minute = 
                    CASE 
                        -- Reset counter if it's been more than a minute
                        WHEN NOW() - last_organic_query_at > interval '1 minute' 
                        THEN 1
                        -- Otherwise increment
                        ELSE organic_queries_last_minute + 1
                    END
            WHERE {dcst.CONTENDER_ID} = ANY($1)
            """,
            [c.id for c in contenders]
        )

    logger.debug(f"Selected {len(contenders)} contenders using round-robin for task {task}")
    return contenders
async def update_contender_timeout(psql_db: PSQLDB, contender: Contender) -> None:
    async with await psql_db.connection() as connection:
        await connection.execute(
            f"""
            UPDATE {dcst.CONTENDERS_TABLE}
            SET last_timeout_at = NOW(),
                timeouts_last_minute = 
                    CASE 
                        WHEN NOW() - last_timeout_at > interval '1 minute' 
                        THEN 1
                        ELSE timeouts_last_minute + 1
                    END
            WHERE {dcst.CONTENDER_ID} = $1
            """,
            contender.id,
        )

async def get_contenders_for_task(connection: Connection, task: str, top_x: int = 5, 
                                  query_type: str = gcst.SYNTHETIC) -> list[Contender]:
    if query_type == gcst.SYNTHETIC:
        return await get_contenders_for_synthetic_task(connection, task, top_x)
    elif query_type == gcst.ORGANIC:
        #return await get_contenders_for_organic_task(connection, task, top_x)
        return await get_contenders_for_synthetic_task(connection, task, top_x)
    else:
        raise ValueError(f"No contender selection strategy have been implemented for query type : {query_type}")
    

async def update_contender_capacities(psql_db: PSQLDB, contender: Contender, capacitity_consumed: float) -> None:
    async with await psql_db.connection() as connection:
        await connection.execute(
            f"""
            UPDATE {dcst.CONTENDERS_TABLE}
            SET {dcst.CONSUMED_CAPACITY} = {dcst.CONSUMED_CAPACITY} + $1, 
                {dcst.TOTAL_REQUESTS_MADE} = {dcst.TOTAL_REQUESTS_MADE} + 1
            WHERE {dcst.CONTENDER_ID} = $2
            """,
            capacitity_consumed,
            contender.id,
        )


async def update_contender_429_count(psql_db: PSQLDB, contender: Contender) -> None:
    async with await psql_db.connection() as connection:
        await connection.execute(
            f"""
            UPDATE {dcst.CONTENDERS_TABLE}
            SET {dcst.REQUESTS_429} = {dcst.REQUESTS_429} + 1,
                {dcst.TOTAL_REQUESTS_MADE} = {dcst.TOTAL_REQUESTS_MADE} + 1
            WHERE {dcst.CONTENDER_ID} = $1
            """,
            contender.id,
        )


async def update_contender_500_count(psql_db: PSQLDB, contender: Contender) -> None:
    async with await psql_db.connection() as connection:
        await connection.execute(
            f"""
            UPDATE {dcst.CONTENDERS_TABLE}
            SET {dcst.REQUESTS_500} = {dcst.REQUESTS_500} + 1,
                {dcst.TOTAL_REQUESTS_MADE} = {dcst.TOTAL_REQUESTS_MADE} + 1
            WHERE {dcst.CONTENDER_ID} = $1
            """,
            contender.id,
        )


async def fetch_contender(connection: Connection, contender_id: str) -> Contender | None:
    row = await connection.fetchrow(
        f"""
        SELECT 
            {dcst.CONTENDER_ID}, {dcst.NODE_HOTKEY}, {dcst.NODE_ID},{dcst.TASK},
            {dcst.CAPACITY}, {dcst.RAW_CAPACITY}, {dcst.CAPACITY_TO_SCORE},
             {dcst.CONSUMED_CAPACITY}, {dcst.TOTAL_REQUESTS_MADE}, {dcst.REQUESTS_429}, {dcst.REQUESTS_500}, 
            {dcst.PERIOD_SCORE}
        FROM {dcst.CONTENDERS_TABLE} 
        WHERE {dcst.CONTENDER_ID} = $1
        """,
        contender_id,
    )
    if not row:
        return None
    return Contender(**row)


async def fetch_all_contenders(connection: Connection, netuid: int | None = None) -> list[Contender]:
    base_query = f"""
        SELECT 
            {dcst.CONTENDER_ID}, {dcst.NODE_HOTKEY}, {dcst.NODE_ID}, {dcst.NETUID}, {dcst.TASK}, 
            {dcst.RAW_CAPACITY}, {dcst.CAPACITY_TO_SCORE}, {dcst.CONSUMED_CAPACITY}, 
            {dcst.TOTAL_REQUESTS_MADE}, {dcst.REQUESTS_429}, {dcst.REQUESTS_500}, 
            {dcst.CAPACITY}, {dcst.PERIOD_SCORE}
        FROM {dcst.CONTENDERS_TABLE}
        """
    if netuid is None:
        rows = await connection.fetch(base_query)
    else:
        rows = await connection.fetch(base_query + f" WHERE {dcst.NETUID} = $1", netuid)
    return [Contender(**row) for row in rows]


async def fetch_hotkey_scores_for_task(connection: Connection, task: str, node_hotkey: str) -> list[PeriodScore]:
    rows = await connection.fetch(
        f"""
        SELECT
            {dcst.NODE_HOTKEY} as hotkey,
            {dcst.TASK},
            {dcst.PERIOD_SCORE},
            {dcst.CONSUMED_CAPACITY},
            {dcst.CREATED_AT}
        FROM {dcst.CONTENDERS_HISTORY_TABLE}
        WHERE {dcst.TASK} = $1
        AND {dcst.NODE_HOTKEY} = $2
        ORDER BY {dcst.CREATED_AT} DESC
        """,
        task,
        node_hotkey,
    )
    return [PeriodScore(**row) for row in rows]


async def update_contenders_period_scores(connection: Connection, netuid: int) -> None:
    rows = await connection.fetch(
        f"""
        SELECT 
            {dcst.CONTENDER_ID},
            {dcst.TOTAL_REQUESTS_MADE},
            {dcst.CAPACITY},
            {dcst.CONSUMED_CAPACITY},
            {dcst.REQUESTS_429},
            {dcst.REQUESTS_500}
        FROM {dcst.CONTENDERS_TABLE}
        WHERE {dcst.NETUID} = $1
    """,
        netuid,
    )

    updates = []
    for row in rows:
        score = calculate_period_score(
            float(row[dcst.TOTAL_REQUESTS_MADE]),
            float(row[dcst.CAPACITY]),
            float(row[dcst.CONSUMED_CAPACITY]),
            float(row[dcst.REQUESTS_429]),
            float(row[dcst.REQUESTS_500]),
        )
        if score is not None:
            updates.append((score, row[dcst.CONTENDER_ID]))

    logger.info(f"Updating {len(updates)} contenders with new period scores")

    await connection.executemany(
        f"""
        UPDATE {dcst.CONTENDERS_TABLE}
        SET {dcst.PERIOD_SCORE} = $1,
            {dcst.UPDATED_AT} = NOW() AT TIME ZONE 'UTC'
        WHERE {dcst.CONTENDER_ID} = $2
    """,
        updates,
    )
    logger.info(f"Updated {len(updates)} contenders with new period scores")


async def get_and_decrement_synthetic_request_count(connection: Connection, contender_id: str) -> int | None:
    """
    Asynchronously retrieves and decrements the synthetic request count for a given contender, setting it to 0 if
    it the consumed capacity is greater than the announced capacity.
    """

    result = await connection.fetchrow(
        f"""
        UPDATE {dcst.CONTENDERS_TABLE}
        SET {dcst.SYNTHETIC_REQUESTS_STILL_TO_MAKE} = 
            CASE 
                WHEN {dcst.CONSUMED_CAPACITY} > {dcst.CAPACITY} THEN 0
                ELSE GREATEST({dcst.SYNTHETIC_REQUESTS_STILL_TO_MAKE} - 1, 0)
            END
        WHERE {dcst.CONTENDER_ID} = $1
        RETURNING {dcst.SYNTHETIC_REQUESTS_STILL_TO_MAKE}
        """,
        contender_id,
    )

    if result:
        return result[dcst.SYNTHETIC_REQUESTS_STILL_TO_MAKE]
    else:
        return None
