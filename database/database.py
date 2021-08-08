import mysql.connector
from dotenv import load_dotenv, find_dotenv
import os

load_dotenv(find_dotenv())

connection = mysql.connector.connect(
  host=os.getenv("DATABASE_HOST"),
  port=os.getenv("DATABASE_PORT"),
  user=os.getenv("DATABASE_USER"),
  password=os.getenv("DATABASE_PASSWORD"),
  database=os.getenv("DATABASE_NAME")
)

def database_insert(table, datas):
    keys = __prepare_datas(datas.keys(), quote='`')
    values = __prepare_datas(datas.values(), quote="'")

    sql = 'INSERT INTO %s (%s) VALUES (%s)' % (table, keys, values)

    try:
        cursor = connection.cursor()

        cursor.execute(sql)

        connection.commit()
    except Exception as ex:
        print(sql)
        print(ex)
        return None

def __prepare_datas(items, quote):
    result = []

    for item in items:
        if item == 'nan' or item == 'null' or item == None:
            result.append('NULL')
        else:
            result.append('%s%s%s' % (quote, item, quote))

    return ", ".join(result)
