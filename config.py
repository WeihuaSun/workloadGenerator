
class PostgreSQLConfig:
    name = "PostgreSQL"
    user = "postgres"
    password = "root"
    host = "localhost"
    port = "5432"
    database = "ischeck"
    is_level = "SERIALIZABLE"

    @ classmethod
    def set_isolation(cls, level = "SERIALIZABLE"):
        return f"SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL {level};"


class MySQLConfig:
    name = "MySQL"
    user = ""
    password = ""
    host = ""
    port = ""
    database = ""

    @ classmethod
    def set_isolation(cls, level):
        return f" SET SESSION TRANSACTION ISOLATION LEVEL {level};"


class MariaDBConfig:
    name = "MariaDB"
    user = ""
    password = ""
    host = ""
    port = ""
    database = ""

    @ classmethod
    def set_isolation(cls, level):
        return f" SET SESSION TRANSACTION ISOLATION LEVEL {level};"


class TiDBConfig:
    name = "TiDB"
    user = ""
    password = ""
    host = ""
    port = ""
    database = ""

    @ classmethod
    def set_isolation(cls, level):
        return f" SET SESSION TRANSACTION ISOLATION LEVEL {level};"


class GenerateConfig:
    database = PostgreSQLConfig
