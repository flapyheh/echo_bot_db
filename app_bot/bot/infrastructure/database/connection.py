import logging
from urllib.parse import quote

from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

logger = logging.getLogger(__name__)

async def build_pg_conninfo(
    db_name: str,
    host: str,
    port: int,
    user: str,
    password: str
) -> str:
    conninfo = (
        f"postgresql://{quote(user, safe='')}:{quote(password, safe='')}"
        f"@{host}:{port}/{db_name}"
    )
    logger.debug(f"Building PostgreSQL connection string (password omitted): "
                 f"postgresql://{quote(user, safe='')}@{host}:{port}/{db_name}")
    return conninfo

async def log_db_version(conn : AsyncConnection):
    try:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT version();")
            db_version = await cursor.fetchone()
            logger.info(f"Connected to PostgreSQL version: {db_version[0]}")
    except Exception as e:
        logger.warning("Failed to fetch DB version: %s", e)

async def get_pg_connection(
    db_name: str,
    host: str,
    port: int,
    user: str,
    password: str
) -> AsyncConnection:
    connection : AsyncConnection | None = None
    connifo = build_pg_conninfo(db_name, host, port, user, password)
    try:
        connection = await AsyncConnection.connect(conninfo= connifo)
        await log_db_version(connection=connection)
        return connection
    except Exception as e:
        logger.exception("Failed to connect to PostgreSQL: %s", e)
        if connection:
            await connection.close()
        raise

async def get_pg_pooldb(
    db_name: str,
    host: str,
    port: int,
    user: str,
    password: str,
    min_size: int = 1,
    max_size: int = 3,
    timeout: float | None = 10.0,
) -> AsyncConnectionPool:
    connection_pool : AsyncConnectionPool | None = None
    conninfo = build_pg_conninfo(db_name, host, port, user, password)
    
    try:
        connection_pool = AsyncConnectionPool(
            conninfo,
            min_size= min_size,
            max_size= max_size,
            timeout= timeout,
            open= False
        )
        await connection_pool.open()
        async with connection_pool.connection() as connection:
            await log_db_version(connection)
    except Exception as e:
        logger.exception("Failed to initialize PostgreSQL pool: %s", e)
        if connection_pool and not connection_pool.close:
            await connection_pool.close()
        raise