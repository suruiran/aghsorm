import { Op, SqlTable, rawsql, runInCtx } from "./index.js";
import { dummydbctx } from "./dummy.js";
import { test } from "vitest";

interface IUserModel {
    id: number;
    name: string;
    bio: string;
    amount: number;
}

const table = new SqlTable<IUserModel>({
    dbctx: dummydbctx,
    schema: "",
    name: "user",
    fields: [
        {
            name: "id",
            sqltype: "integer",
            nullable: false,
            isprimary: true,
            default: null,
            comment: ""
        },
        {
            name: "name",
            sqltype: "text",
            nullable: false,
            isprimary: false,
            default: null,
            comment: ""
        },
        {
            name: "bio",
            sqltype: "text",
            nullable: false,
            isprimary: false,
            default: null,
            comment: ""
        },
    ],
    ddl: {} as any,
});

test("insert", () => {
    runInCtx("", dummydbctx, () => {
        table.insert({
            id: 1,
            name: "0.0",
            bio: "hahaha",
        }).export();
    });
});

test("update", () => {
    runInCtx("", dummydbctx, () => {
        table.update(
            {
                id: table.colop("id").lte(1),
            },
            {
                id: 1,
                name: "0.0",
                bio: "hahaha",
            }).export();
    });
});

test("delete", () => {
    runInCtx("", dummydbctx, () => {
        table.delete(
            table.colop("id").lte(1),
            {
                orderby: [{ field: "id", direction: "ASC" }]
            }
        ).export();
    });
});

test("select", () => {
    runInCtx("", dummydbctx, () => {
        table.select(
            {},
            {
                include: [
                    Op.call("count", table.colop("id")).alias("count"),
                    rawsql`sum(amount) as total_amount`.op(),
                ]
            }
        ).export();
    });
});

test("equals", () => {
    runInCtx("", dummydbctx, () => {
        table.select(table.equals({ id: 1, name: "xxx", bio: "hahaha" })).export();
    });
});
