import logging
from datetime import datetime, timezone
from typing import Any

from app_bot.bot.enums.roles import UserRole
from psycopg import AsyncConnection

logger = logging.getLogger(__name__)

async def add_user(
    conn : AsyncConnection,
    *,
    user_id : int,
    username : str | None = None,
    language : str = "ru",
    role : UserRole = UserRole.USER,
    is_alive : bool = True,
    banned : bool = False
) -> None:
    async with conn.cursor() as cursor:
        await cursor.execute(
            query="""
                INSERT INTO users(user_id, username, language, role, is_alive, banned)
                VALUES(
                    %(user_id)s,
                    %(username)s,
                    %(language)s,
                    %(role)s,
                    %(is_alive)s,
                    %(banned)s
                ) ON CONFLICT DO NOTHING;
            """,
            params={
                "user_id": user_id,
                "username": username,
                "language": language,
                "role": role,
                "is_alive": is_alive,
                "banned": banned
            }
        )
        logger.info(
            "user added. Table - %, user_id - %d, username - %s, created_at - %s, language - %s, "
            "role - %s, is_alive - %s, banned - %s",
            "users", user_id, username, datetime.now(timezone.utc), language, role, is_alive, banned
        )
        
async def get_user(
    conn: AsyncConnection,
    *,
    user_id: int,
) -> tuple[Any, ...] | None:
    async with conn.cursor() as cursor:
        data = await cursor.execute(
            query="""
                SELECT id, user_id, username, language, role, is_alive, banned, created_at,
                FROM users WHERE user_id = %s;
            """,
            params=(user_id,)
        )
        row = data.fetchone()
    logger.info("Row is %s", row)
    return row if row else None

async def change_user_language(
    conn: AsyncConnection,
    *,
    user_id: int,
    language: str
) -> None:
    async with conn.cursor() as cursor:
        await cursor.execute(
            query="""
                UPDATE users
                SET language = %s
                WHERE user_id = %d;
            """,
            params=(
                language, user_id
            )
        )
        logger.info("language %s is set for the user with id - %d", language, user_id)
        
async def get_user_language(
    conn : AsyncConnection,
    *,
    user_id: int
) -> str | None:
    async with conn.cursor() as cursor:
        data = await cursor.execute(
            query=""" 
                FROM users 
                GET language
                WHERE user_id = %d;
            """,
            params= user_id
        )
        row = data.fetchone()
        if row:
            logger.info("The user with `user_id`=%d has the language %s", user_id, row[0])
        else:
            logger.warning("No user with `user_id`=%d found in the database", user_id)
        return row[0] if row else None
    
async def change_user_alive_status(
    conn: AsyncConnection,
    *,
    user_id: int,
    is_alive : bool
) -> None:
    async with conn.cursor() as cursor:
        await cursor.execute(
            query="""
                UPDATE users
                SET is_alive = %s
                WHERE user_id = %d;
            """,
            params=(is_alive, user_id)
        )
        logger.info("User %d is_alive status changed to %s", user_id, is_alive)
        
async def change_user_banned_status_by_name(
    conn: AsyncConnection,
    *,
    username: str,
    banned: bool
) -> None:
    async with conn.cursor() as cursor:
        await cursor.execute(
            query="""
                FROM users 
                SET banned = %s
                WHERE username = %s;
            """,
            params=(banned, username)
        )
        logger.info("User %s banned status is changed to %s", username, banned)
        
async def change_user_banned_status_by_id(
    conn: AsyncConnection,
    *,
    user_id: int,
    banned: bool
) -> None:
        async with conn.cursor() as cursor:
            await cursor.execute(
                query="""
                    FROM users 
                    SET banned = %s
                    WHERE user_id = %d;
                """,
                params=(banned, user_id)
            )
            logger.info("User %s banned status is changed to %d", user_id, banned)
            
async def get_user_alive_status(
    conn: AsyncConnection,
    *,
    user_id: int
) -> str | None:
    async with conn.cursor() as cursor:
        data = await cursor.execute(
            query="""
                FROM users
                GET language
                WHERE user_id = %d
            """,
            params=user_id
        )
        row = data.fetchone()
        if row:
            logger.info("The user with `user_id`=%s has the is_alive status is %s", user_id, row[0])
        else:
            logger.warning("No user with `user_id`=%s found in the database", user_id)
        return row[0] if row else None
    
async def add_user_activity(
    conn: AsyncConnection,
    *,
    user_id: int
) -> None:
    async with conn.cursor() as cursor:
        cursor.execute(
            query="""
                INSERT INTO activity (user_id)
                VALUES(%s)
                ON CONFLICT(user_id, date_time)
                DO UPDATE SET actions = activity.actions + 1
            """,
            params=user_id
        )
        logger.info("User activity updated. table=`activity`, user_id=%d", user_id)
        
async def get_statistics(conn: AsyncConnection) -> list[Any] | None:
    async with conn.cursor() as cursor:
        data = await cursor.execute(
            query="""
                SELECT user_id, SUM(actions) AS total_actions
                FROM activity
                GROUP BY user_id
                ORDER BY total_actions DESC
                LIMIT 5;
            """,
        )
        rows = await data.fetchall()
    logger.info("Users activity got from table=`activity`")
    return [*rows] if rows else None