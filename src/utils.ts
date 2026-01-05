import { ILimitOptions, IOffsetOptions, IOrderOptions } from "./crud.js";
import { Fragment, Frags, mksqlfrag, mkvalfrag } from "./frag.js";
import { IOpableItems, Op } from "./op.js";
import { Identifier, RawSql } from "./types.js";

export function opItemToSQL(item: IOpableItems, temp: Fragment[]) {
    if (item instanceof Identifier) {
        item.op().tosql(temp)
        return;
    }
    if (item instanceof RawSql) {
        temp.push(...item.frags);
        return;
    }
    if (item instanceof Op) {
        item.tosql(temp)
        return;
    }
    temp.push(mkvalfrag(item));
}



export function pushOrders<T>(temp: Fragment[], opts?: IOrderOptions<T>) {
    if (!opts || !opts.orderby) return;
    temp.push(Frags.orderby);
    const size = opts.orderby.length;
    let idx = 0;
    for (const item of opts.orderby) {
        if (typeof item === "string") {
            temp.push(mksqlfrag(dbctx.quote(null, item)));
        } else {
            temp.push(mksqlfrag(`${dbctx.quote(null, item.field)} ${item.direction}`));
        }
        idx++;
        if (idx < size) {
            temp.push(Frags.comma);
        }
    }
}

export function pushLimitOffset(
    temp: Fragment[],
    opts?: ILimitOptions & IOffsetOptions
) {
    if (!opts) return;
    if (opts.limit) {
        temp.push(Frags.limit);
        temp.push(mkvalfrag(opts.limit));
    }
    if (opts.offset) {
        temp.push(Frags.offset);
        temp.push(mkvalfrag(opts.offset));
    }
}

