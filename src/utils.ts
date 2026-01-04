import { Fragment, Identifier, ILimitOptions, IOffsetOptions, IOpableItems, IOrderOptions, RawSql } from "./index.js";
import { Op } from "./op.js";

export function opItemToSQL(item: IOpableItems, temp: Fragment[]) {
    if (item instanceof Identifier) {
        item.op().tosql(temp)
        return;
    }
    if (item instanceof RawSql) {
        temp.push({ sql: item._sql });
        return;
    }
    if (item instanceof Op) {
        item.tosql(temp)
        return;
    }
    temp.push({ value: item });
}

export function pushOrders<T>(temp: Fragment[], opts?: IOrderOptions<T>) {
    if (!opts || !opts.orderby) return;
    temp.push({ sql: "ORDER BY" });
    const size = opts.orderby.length;
    let idx = 0;
    for (const item of opts.orderby) {
        if (typeof item === "string") {
            temp.push({ sql: dbctx.quote(null, item) });
        } else {
            temp.push({
                sql: `${dbctx.quote(null, item.field)} ${item.direction}`,
            });
        }
        idx++;
        if (idx < size) {
            temp.push({ sql: "," })
        }
    }
}

export function pushLimitOffset(
    temp: Fragment[],
    opts?: ILimitOptions & IOffsetOptions
) {
    if (!opts) return;
    if (opts.limit) {
        temp.push({ sql: `LIMIT` });
        temp.push({ value: opts.limit });
    }
    if (opts.offset) {
        temp.push({ sql: `OFFSET` });
        temp.push({ value: opts.offset });
    }
}