import { SqlTable, runInCtx } from "./index.js";
import { dummydbctx } from "./dummy.js";
import { test } from "vitest";

interface IUserModel {
    id: number;
    name: string;
    bio: string;
}

const table = new SqlTable<IUserModel, ["id"]>({
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
    indexes: [],
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