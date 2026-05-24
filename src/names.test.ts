import { expect, test } from "vitest";
import { ClickhouseDialect } from "./ddl.clickhouse.js";
import { DuckdbDialect } from "./ddl.duckdb.js";
import { Ibmdb2Dialect } from "./ddl.ibmdb2.js";
import { MssqlDialect } from "./ddl.mssql.js";
import { MysqlDialect } from "./ddl.mysql.js";
import { OracleDialect } from "./ddl.oracle.js";
import { PostgreSQLDialect } from "./ddl.postgresql.js";
import { SqliteDialect } from "./ddl.sqlite.js";

test("ensure classname", () => {
    for (const cls of [ClickhouseDialect, DuckdbDialect, Ibmdb2Dialect, MssqlDialect, MysqlDialect, OracleDialect, PostgreSQLDialect, SqliteDialect]) {
        expect(cls.ClassName === cls.name).toBe(true);
    }
});