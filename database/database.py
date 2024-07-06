import time

import psycopg2
import mysql.connector
import mariadb
from config import *
from utils import *


from database.operator import *
from database.transaction import Transaction


class DBManager:
    def __init__(self, config):
        self.config = config
        self.connect_pool = []
        self.tid_counter = SharedInt()
        self.oid_counter = SharedInt()
        self.is_level = "SERIALIZABLE"
        if config == PostgreSQLConfig:
            self.connector = psycopg2.connect
            self.is_level = PostgreSQLConfig.is_level
        elif config == TiDBConfig:
            self.connector = mysql.connector.connect
        elif config == MySQLConfig:
            self.connector = mysql.connector.connect
        elif config == MariaDBConfig:
            self.connector = mariadb.connect
        else:
            print("Unexpected DB!")

    def connect(self, init=False):
        try:
            conn = self.connector(
                user=self.config.user,
                password=self.config.password,
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                options="-c client_encoding=UTF8"
            )
            conn.autocommit = False

            cursor = conn.cursor()
            cursor.execute(self.config.set_isolation(self.is_level))
            conn.commit()

            cursor.execute("SELECT version();")
            record = cursor.fetchone()[0]
            conn.commit()
            cursor.close()

            # #print(f"Connected to Database {self.config.name} - {record}")
            # #print(f"Isolation level : {is_level}")
            connection = Connection(
                conn, self.tid_counter, self.oid_counter, init)
            self.connect_pool.append(connection)
            return connection
        except Exception as error:
            print(f"Error connecting to {self.config.name}: {error}")

    def close(self):
        while self.connect_pool:
            conn = self.connect_pool.pop()
            conn.close()


class Connection:
    def __init__(self, conn, tid_counter: SharedInt, oid_counter: SharedInt, init=False):
        self.conn = conn
        self.tid_counter = tid_counter
        self.oid_counter = oid_counter
        self.cursor = self.conn.cursor()
        self.init = init
        self.transaction = None

    def close(self):
        self.cursor.close()
        self.conn.close()

    def create_table(self, table, key_len, value_len):
        self.cursor.execute(
            f"""create table {table}(
                key varchar({key_len}) primary key,
                value varchar({value_len}));
            """)
        self.conn.commit()

    def drop_table(self, table):
        self.cursor.execute(f"drop table if EXISTS {table};")
        self.conn.commit()

    def begin(self):
        if self.init:
            self.cursor.execute("BEGIN;")
        else:
            oid = self.oid_counter.increment()
            start = time.time_ns()
            self.cursor.execute("BEGIN;")
            end = time.time_ns()
            begin = Begin(oid, start, end)
            self.transaction = Transaction(self.tid_counter.increment())
            self.transaction.set_start(start)
            self.transaction.add(begin)
        return self.transaction

    def commit(self):
        try:
            if self.init:
                self.conn.commit()
            else:
                oid = self.oid_counter.increment()
                start = time.time_ns()
                self.conn.commit()
                end = time.time_ns()
                commit = Commit(oid, start, end)
                self.transaction.set_end(end)
                self.transaction.add(commit)
                if self.transaction.start == self.transaction.end:
                    self.transaction.end += 1
                    self.transaction.start -= 1
            return True
        except Exception as e:
            # print(f"Commit error:{e}")
            self.abort()
            return False

    def abort(self):
        try:
            # print("Abort")
            if self.init:
                self.conn.rollback()
            else:
                oid = self.oid_counter.increment()
                start = time.time_ns()
                self.conn.rollback()
                end = time.time_ns()
                abort = Abort(oid, start, end)
                self.transaction.set_end(end)
                self.transaction.add(abort)
                if self.transaction.start == self.transaction.end:
                    self.transaction.end += 1
                    self.transaction.start -= 1
            return True
        except:
            return False

    def insert(self, table, key, value):
        try:
            if self.init:
                oid = 0
                tid = 0
            else:
                oid = self.oid_counter.increment()
                tid = self.transaction.tid
            packed_value = pack_value(value, tid, oid)
            packed_key = pack_key(table, key)
            sql = f"INSERT INTO {table} (key, value) VALUES ('{key}','{packed_value}') ;"
            start = time.time_ns()
            self.cursor.execute(sql)
            if not self.init:
                end = time.time_ns()
                insert = Write(oid, start, end, packed_key)
                self.transaction.add(insert)
            return True
        except Exception as e:
            # print(f"Insert error:{e}")
            # print(sql)
            self.abort()
            return False

    def delete(self, table, key):
        try:
            oid = self.oid_counter.increment()
            sql = f"DELETE FROM {table} WHERE key = '{key}' ;"
            start = time.time_ns()
            self.cursor.execute(sql)
            end = time.time_ns()
            packed_key = pack_key(table, key)
            delete = Write(oid, start, end, packed_key)
            self.transaction.add(delete)
            return True
        except Exception as e:
            # print(f"Delete error:{e}")
            self.abort()
            # print(sql)
            return False

    def get(self, table, key):
        try:
            oid = self.oid_counter.increment()
            sql = f"SELECT value FROM {table} WHERE key = '{key}' ;"
            start = time.time_ns()
            self.cursor.execute(sql)
            end = time.time_ns()
            rows = self.cursor.fetchone()
            if rows is None or (len(rows) == 0):
                return None
            packed_value = rows[0]
            value, from_tid, from_oid = unpack_value(packed_value)
            packed_key = pack_key(table, key)
            get = Read(oid, start, end, packed_key, from_tid, from_oid)
            self.transaction.add(get)
            return value
        except Exception as e:
            # print(f"Select error:{e}")
            # print(sql)
            self.abort()
            return False

    def set(self, table, key, value):
        try:
            if self.init:
                oid = 0
                tid = 0
            else:
                oid = self.oid_counter.increment()
                tid = self.transaction.get_id()
            packed_key = pack_key(table, key)
            packed_value = pack_value(value, tid, oid)
            sql = f"UPDATE {table} SET value = '{packed_value}' WHERE key = '{key}' ;"
            start = time.time_ns()
            self.cursor.execute(sql)
            end = time.time_ns()
            set = Write(oid, start, end, packed_key)
            self.transaction.add(set)
            return True
        except Exception as e:
            # print(f"Update error:{e}")
            # print(sql)
            self.abort()
            return False
