import { Fragment, mkvalfrag } from "./frag.js";
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
