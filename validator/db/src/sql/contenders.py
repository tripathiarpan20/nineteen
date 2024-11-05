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
    Load-adaptive contender selection that switches between quality-focused and pure load balancing modes 
    based on system load.
    """
    # First check current load metrics
    load_info = await connection.fetchrow(
        f"""
        SELECT 
            COUNT(DISTINCT node_hotkey) as active_nodes,
            SUM(CASE WHEN updated_at > NOW() - INTERVAL '10 seconds' THEN 1 ELSE 0 END) as recent_requests,
            AVG(consumed_capacity::float / NULLIF(capacity, 0)) as avg_capacity_usage
        FROM {dcst.CONTENDERS_TABLE}
        WHERE task = $1
        AND capacity > 0
        """,
        task
    )

    active_nodes = load_info['active_nodes'] or 0
    recent_requests = load_info['recent_requests'] or 0
    avg_capacity_usage = load_info['avg_capacity_usage'] or 0

    # If we're under high load, switch to pure load balancing mode
    high_load = recent_requests > active_nodes * 0.5 or avg_capacity_usage > 0.7

    if high_load:
        logger.debug("System under high load - using pure load balancing mode")
        rows = await connection.fetch(
            f"""
            WITH base_contenders AS (
                SELECT 
                    c.{dcst.CONTENDER_ID},
                    c.{dcst.NODE_HOTKEY},
                    c.{dcst.NODE_ID},
                    c.{dcst.TASK},
                    c.{dcst.RAW_CAPACITY},
                    c.{dcst.CAPACITY_TO_SCORE},
                    c.{dcst.CONSUMED_CAPACITY},
                    c.{dcst.TOTAL_REQUESTS_MADE},
                    c.{dcst.REQUESTS_429},
                    c.{dcst.REQUESTS_500},
                    c.{dcst.CAPACITY},
                    c.{dcst.PERIOD_SCORE},
                    c.{dcst.NETUID},
                    c.{dcst.UPDATED_AT},
                    -- Calculate time since last use
                    EXTRACT(EPOCH FROM (NOW() - c.{dcst.UPDATED_AT})) as seconds_since_update,
                    -- Calculate recent error rate
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
            ranked_contenders AS (
                SELECT *,
                    ROW_NUMBER() OVER (
                        ORDER BY 
                            -- Prioritize nodes that haven't been used recently
                            seconds_since_update DESC,
                            -- Then consider error rate as secondary factor
                            error_rate ASC,
                            -- Add some randomization to spread load
                            random()
                    ) as rank
                FROM base_contenders
                WHERE
                    -- Basic availability checks
                    error_rate < 0.3
                    -- Ensure some cooldown between requests
                    AND seconds_since_update > 1
            )
            SELECT * FROM ranked_contenders
            LIMIT $2 * 2
            """,
            task,
            top_x
        )
    else:
        # Under normal load, use quality-aware selection
        rows = await connection.fetch(
            f"""
            WITH latest_stats AS (
                SELECT DISTINCT ON (node_hotkey, task)
                    node_hotkey,
                    task,
                    {dcst.COLUMN_NORMALISED_NET_SCORE} as performance_score
                FROM {dcst.CONTENDERS_WEIGHTS_STATS_TABLE}
                WHERE task = $1
                ORDER BY node_hotkey, task, created_at DESC
            )
            SELECT 
                c.{dcst.CONTENDER_ID},
                c.{dcst.NODE_HOTKEY},
                c.{dcst.NODE_ID},
                c.{dcst.TASK},
                c.{dcst.RAW_CAPACITY},
                c.{dcst.CAPACITY_TO_SCORE},
                c.{dcst.CONSUMED_CAPACITY},
                c.{dcst.TOTAL_REQUESTS_MADE},
                c.{dcst.REQUESTS_429},
                c.{dcst.REQUESTS_500},
                c.{dcst.CAPACITY},
                c.{dcst.PERIOD_SCORE},
                c.{dcst.NETUID},
                s.performance_score * 
                CASE 
                    WHEN c.{dcst.CONSUMED_CAPACITY}::float / NULLIF(c.{dcst.CAPACITY}, 0) > 0.8 THEN 0.2
                    WHEN c.{dcst.CONSUMED_CAPACITY}::float / NULLIF(c.{dcst.CAPACITY}, 0) > 0.5 THEN 0.5
                    ELSE 1.0
                END as adjusted_score
            FROM {dcst.CONTENDERS_TABLE} c
            JOIN {dcst.NODES_TABLE} n ON c.{dcst.NODE_ID} = n.{dcst.NODE_ID} 
                AND c.{dcst.NETUID} = n.{dcst.NETUID}
            JOIN latest_stats s ON c.{dcst.NODE_HOTKEY} = s.node_hotkey 
                AND c.{dcst.TASK} = s.task
            WHERE c.{dcst.TASK} = $1
            AND c.{dcst.CAPACITY} > 0
            AND n.{dcst.SYMMETRIC_KEY_UUID} IS NOT NULL
            AND (NOW() - c.{dcst.UPDATED_AT}) > interval '2 seconds'
            ORDER BY adjusted_score DESC
            LIMIT $2 * 2
            """,
            task,
            top_x
        )

    if not rows:
        logger.debug(f"No valid contenders found for organic query with task {task}, falling back to synthetic queries logic.")
        return await get_contenders_for_synthetic_task(connection, task, top_x)

    # Convert rows to contenders
    contenders = []
    seen_hotkeys = set()
    
    for row in rows:
        if row[dcst.NODE_HOTKEY] in seen_hotkeys:
            continue
            
        contender = Contender(
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
        contenders.append(contender)
        seen_hotkeys.add(row[dcst.NODE_HOTKEY])
        
        if len(contenders) >= top_x:
            break

    # Update timestamps for selected contenders
    if contenders:
        await connection.execute(
            f"""
            UPDATE {dcst.CONTENDERS_TABLE}
            SET {dcst.UPDATED_AT} = NOW()
            WHERE {dcst.CONTENDER_ID} = ANY($1)
            """,
            [c.id for c in contenders]
        )

    if len(contenders) < top_x:
        logger.debug(f"Not enough unique organic contenders ({len(contenders)} < {top_x}), falling back to synthetic queries logic")
        return await get_contenders_for_synthetic_task(connection, task, top_x)

    random.shuffle(contenders)  # Final shuffle to prevent patterns
    logger.debug(f"Selected {len(contenders)} unique contenders for task {task} (high_load={high_load})")
    return contenders[:top_x]


async def get_contenders_for_task(connection: Connection, task: str, top_x: int = 5, 
                                  query_type: str = gcst.SYNTHETIC) -> list[Contender]:
    if query_type == gcst.SYNTHETIC:
        return await get_contenders_for_synthetic_task(connection, task, top_x)
    elif query_type == gcst.ORGANIC:
        return await get_contenders_for_organic_task(connection, task, top_x)
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
