import re
import asyncio
import os
import sys
import traceback
import warnings
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from pathlib import Path

import asyncpg
import pandas as pd
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine


# --- 1. Конфигурация и загрузка переменных окружения ---
load_dotenv()  # Загружаем переменные окружения из .env файла

USERNAME = os.getenv("PG_USER")  # Пользователь PostgreSQL
PSW = os.getenv("PG_PASSWORD")  # Пароль PostgreSQL
BASENAME = os.getenv("PG_DBNAME")  # Имя БД PostgreSQL
HOSTNAME_PUBLIC = os.getenv("PG_HOST_LOCAL")  # Хост PostgreSQL
PORT = os.getenv("PG_PORT")  # Порт PostgreSQL
URL_CONST = os.getenv("URL_1C")

CONN_PARAMS = {
    "user": USERNAME,
    "password": PSW,
    "database": BASENAME,
    "host": HOSTNAME_PUBLIC,
    "PORT": PORT
}


warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy connectable")

pg_engine_async = create_async_engine(
    'postgresql+asyncpg://%s:%s@%s:%s/%s' % (USERNAME, PSW, HOSTNAME_PUBLIC, PORT, BASENAME),
    echo=True)

engine = create_engine('postgresql://%s:%s@%s:%s/%s' % (USERNAME, PSW, HOSTNAME_PUBLIC, PORT, BASENAME))

# Create a ThreadPoolExecutor
executor = ThreadPoolExecutor(max_workers=4)


# обработчик, для получения из базы одной записи/строки
async def get_result_one_column(sql, *args):
    conn = await asyncpg.connect(**CONN_PARAMS)
    try:
        if len(args) == 0:
            result = await conn.fetchval(sql)
        else:
            result = await conn.fetchval(sql, *args)
        return result
    except Exception as e:
        sms = "ERROR:get_result_one_column: %s " % e
        print(sms)

    finally:
        if conn and not conn.is_closed():
            await conn.close()

    return None


# обработчик, для получения из базы одной записи/строки
async def get_result_one_row(sql, *args):
    conn = await asyncpg.connect(**CONN_PARAMS)
    try:
        if len(args) == 0:
            records = await conn.fetchrow(sql)
        else:
            records = await conn.fetchrow(sql, *args)

        record_dict = dict(records)
        df = pd.DataFrame([record_dict])
        return df
    except Exception as e:
        sms = "ERROR:get_result_one_row: %s " % e
        print(sms)

    finally:
        if conn and not conn.is_closed():
            await conn.close()

    return None


async def df_to_sql(df, table_name):
    loop = asyncio.get_event_loop()
    func = partial(df.to_sql, table_name, engine, if_exists='append', index=False, method='multi')
    await loop.run_in_executor(executor, func)


async def df_to_sql2(df, table_name, executor=None):
    # for python >= 3.9
    if executor:
        loop = asyncio.get_event_loop()
        func = partial(df.to_sql, table_name, engine, if_exists='append', index=False, method='multi')
        await loop.run_in_executor(executor, func)
    else:
        await asyncio.to_thread(df.to_sql, table_name, engine, if_exists='append', index=False, method='multi')


async def df_to_sql3(df, table_name, executor=None):
    from functools import partial
    loop = asyncio.get_event_loop()
    func = partial(df.to_sql, table_name, engine, if_exists='append', index=False, method='multi')
    await loop.run_in_executor(executor, func)


async def sql_to_df(table_name):
    df = pd.read_sql(f"SELECT * FROM {table_name}", engine)
    return df


async def get_df(sql)->pd.DataFrame:
    conn = await asyncpg.connect(**CONN_PARAMS)
    try:
        records = await conn.fetch(sql)
        df = pd.DataFrame([dict(record) for record in records])
        return df
    finally:
        await conn.close()


async def clear_table(table_name):
    sql = f"DELETE FROM {table_name}"
    await async_save_pg(sql)


def clear_table_sync(table_name):
    """Синхронная версия функции для очистки таблицы"""
    import asyncio
    return asyncio.run(clear_table(table_name))


async def create_model_async(fields_count):
    model = []
    for i in range(fields_count):
        model.append(f"${i + 1}")
    return tuple(model)


async def sql_to_df_async(sql):
    conn = await asyncpg.connect(user=USERNAME, password=PSW, database=BASENAME, host=HOSTNAME_PUBLIC, PORT=PORT)
    try:
        records = await conn.fetch(sql)
        df = pd.DataFrame([dict(record) for record in records])
        return df
    finally:
        await conn.close()


async def async_save_pg(sql, *args):
    conn = None
    result = False
    try:
        if not conn or conn.is_closed():
            conn = await asyncpg.connect(**CONN_PARAMS, timeout=300)

        tr = conn.transaction()
        await tr.start()

        if args:
            await conn.executemany(sql, *args)
        else:
            await conn.execute(sql)

    except Exception as e:
        # await tr.rollback()
        # sms = traceback.print_exc()
        print(sql)
        print(*args)
        print(f"ERROR: {e}")
        raise

    else:
        await tr.commit()
        result = True

    finally:
        if conn and not conn.is_closed():
            try:
                await conn.close(
                    timeout=60
                )  # Увеличиваем таймаут для закрытия соединения
            except Exception as e:
                print(f"Ошибка при закрытии соединения: {e}")
        return result


# get connection to database postgres. connection type psycopg2
def con_postgres_psycopg2():
    conn = ''
    try:
        import psycopg2
        conn = psycopg2.connect(**CONN_PARAMS)
    except Exception as e:
        sms = "ERROR:ConnectToBase:dfConPostgresPsycopg2: %s" % e
        print(sms)

    finally:
        return conn


def save_to_pg(sql, *args):
    conn = con_postgres_psycopg2()
    try:
        cur = conn.cursor()
        if args:
            cur.executemany(sql, *args)
        else:
            cur.execute(sql)
        conn.commit()
        return True
    except Exception as e:
        sms = "ERROR:ConnectToBase:save_to_pg: %s" % e
        print(sms)
        return False
    finally:
        cur.close()
        conn.close()


def get_json_from_url(url_list):
    try:
        resp = requests.get(url_list)
    except Exception as e:
        print(f"Возникла ошибка при получении данных из url: {url_list}")
    else:
        result = resp.json()
        return result


def run_map_get_list(lists):
    with ProcessPoolExecutor(max_workers=6) as executor:
        result_list = list(executor.map(get_json_from_url, lists))
        return result_list


async def main_thread(urls_list):
    list_parent = []
    responses = run_map_get_list(urls_list)
    for item in responses:
        list_child = []
        for item2 in item:
            list_child = tuple(item2.values())
        list_parent.append(list_child)

    return list_parent


async def dict_to_sql_unqkey_async(table_name, mydict, unqkey):
    # данные json переводит в sql insert формат с учетом уникальности данных
    # данные в базу НЕ заносит!!!

    strsql = ''
    odata = list()
    try:
        placeholders = await create_model_async(len(mydict))
        placeholders = re.sub(r"'", "", str(placeholders))  # remove all quotes
        columns = ', '.join(mydict.keys())
        odata = list(mydict.values())
        strsql = f'''INSERT INTO {table_name} ({columns}) VALUES {placeholders}
                ON CONFLICT ON CONSTRAINT {unqkey}
                DO NOTHING             
            '''

    except Exception as e:
        msj = "ERROR:ConnectToBase:dict_to_sql: %s" % e
        print(msj)

    finally:
        return strsql.lower(), odata
