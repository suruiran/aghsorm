import type { DBContext, Fragment, Value, IColumn } from "./index.js";

export const dummydbctx: DBContext = {
    quote: function (table: string | null, column: string | null): string {
        if (table && column) return `${table}.${column}`;
        if (table) return table;
        return column!;
    },
    render: function (fragments: Fragment[]): [string, Value[]] {
        const tmp = [] as string[];
        const args = [] as Value[];
        for (const ele of fragments) {
            if (ele.sql) {
                tmp.push(ele.sql);
                continue;
            }
            tmp.push(`\$${args.length + 1}`);
            args.push(ele.value!);
        }
        return [tmp.join(" "), args];
    },
};