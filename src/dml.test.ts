import { Op, SqlTable, rawsql } from "./index.js";
import { dummydbctx } from "./dummy.js";
import { test } from "vitest";
import { ThisCol } from "./op.js";

interface IUserModel {
    id: number;
    name: string;
    bio: string;
    amount: number;
}

const table = new SqlTable<IUserModel>(
    dummydbctx,
    {
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
            {
                name: "amount",
                sqltype: "integer",
                nullable: false,
                isprimary: false,
                default: null,
                comment: ""
            }
        ],
        ddl: {} as any,
    });

test("insert", () => {
    table.insert({
        id: 1,
        name: "0.0",
        bio: "hahaha",
    }).export(dummydbctx);
});

test("update", () => {
    table.update(
        {
            id: ThisCol.lte(1),
        },
        {
            id: 1,
            name: "0.0",
            bio: "hahaha",
        }).export(dummydbctx);
});

test("delete", () => {
    table.delete(
        table.colop("id").lte(1),
        {
            orderby: [{ field: "id", direction: "ASC" }]
        }
    ).export(dummydbctx);
});

test("select", () => {
    const percent = `percent` as any as keyof IUserModel;
    table.select(
        {},
        {
            include: [
                rawsql`( ${table.column("amount").string()} / sum(amount) ) * 100`.op().alias(dummydbctx, percent),
            ],
            orderby: [{
                field: percent,
                direction: "DESC",
            }],
            limit: 100,
        }
    ).export(dummydbctx);
});

test("equals", () => {
    table.select(table.equals({ id: 1, name: "xxx", bio: "hahaha" })).export(dummydbctx);
});


test("all op methods", () => {
    const idCol = table.colop("id");
    const amountCol = table.colop("amount");
    const nameCol = table.colop("name");
    const bioCol = table.colop("bio");

    const condition = Op.and(
        idCol.gt(10).and(idCol.lt(100)),
        Op.or(
            nameCol.eq("Alice"),
            nameCol.like("%Bob%")
        ),
        bioCol.isnull().not(),
        amountCol.between(100, 200).bracket(),
        idCol.in(1, 2, 3, 5, 8).bracket(),
        idCol.notin(10, 20, 30).bracket(),
        amountCol.gte(0).bracket(),
    );

    const calc1 = idCol.plus(amountCol).alias(dummydbctx, "plus_col");
    const calc2 = idCol.minus(amountCol).alias(dummydbctx, "minus_col");
    const calc3 = idCol.multiply(amountCol).alias(dummydbctx, "multiply_col");
    const calc4 = idCol.divide(amountCol).alias(dummydbctx, "divide_col");
    const calc5 = idCol.mod(amountCol).alias(dummydbctx, "mod_col");
    const calc6 = idCol.pow(Op.bracket(amountCol.divide(100))).alias(dummydbctx, "pow_col");
    const calc7 = idCol.lshift(2).alias(dummydbctx, "lshift_col");
    const calc8 = idCol.rshift(2).alias(dummydbctx, "rshift_col");

    const func1 = Op.call("COALESCE", bioCol, "default bio").alias(dummydbctx, "coalesce_col");
    const func2 = Op.call("LENGTH", nameCol).alias(dummydbctx, "name_len");

    table.select(
        condition,
        {
            include: [
                calc1, calc2, calc3, calc4, calc5, calc6, calc7, calc8,
                func1, func2,
            ],
            orderby: [{ field: "id", direction: "ASC" }],
            limit: 10,
        }
    ).export(dummydbctx);
});

test("groupby and having", () => {
    const idCol = table.colop("id");
    const amountCol = table.colop("amount");
    const nameCol = table.colop("name");

    const where = amountCol.gt(0);

    const totalAmount = table.fakecol("total_amount");

    const include = [
        nameCol.alias(dummydbctx, "user_name"),
        Op.call("SUM", amountCol).alias(dummydbctx, totalAmount),
        Op.call("AVG", amountCol).alias(dummydbctx, "avg_amount"),
        Op.call("COUNT", idCol).alias(dummydbctx, "user_count"),
    ];

    const having = Op.and(
        Op.call("SUM", amountCol).gt(500),
        Op.call("COUNT", idCol).gte(2),
    );

    table.select(where, {
        include,
        groupby: ["name"],
        having,
        orderby: [{ field: totalAmount, direction: "DESC" }],
        limit: 50,
    }).export(dummydbctx);
});

test("colop", () => {
    table.select({
        id: ThisCol.lte(1)
    }).export(dummydbctx);
});