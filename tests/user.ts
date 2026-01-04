import { Table, Op } from "../src/index.ts";
import { dummydbctx } from "../src/dummy.ts"

globalThis.dbctx = dummydbctx;

interface User {
    id: number;
    name: string;
    age: number;
    birthday: Date;
    avatar: Uint8Array;
    is_admin: boolean;
    created_at: Date;
}

const users = new Table<User, ["id"]>("user", [], []);

console.log(
    dbctx.render(
        users.delete(
            users.field("id").gt(Op.plus(1, 2).bracket()),
            {
                orderby: ["age"],
                limit: 10,
            }
        )
    )
);


console.log(
    dbctx.render(
        users.update(
            { age: Op.plus(users.field("age"), 1), created_at: Op.call("NOW") },
            Op.gte(users.field("id"), 12).and(
                users.equals({ is_admin: true })
            ),
        )
    )
)

console.log(
    dbctx.render(
        users.select(
            Op.gte(users.field("id"), 12).and(
                users.equals({ is_admin: true })
            ),
        )
    )
)