import { type Fragment, mksqlfrag, mkvalfrag } from "./frag.js";
import { lazy } from "./lazy.js";
import type { IOpableItems } from "./op.js";

export function opItemToSQL(item: IOpableItems, temp: Fragment[]) {
    if (item instanceof lazy.Identifier) {
        item.op().tosql(temp)
        return;
    }
    if (item instanceof lazy.RawSql) {
        temp.push(...item.frags);
        return;
    }
    if (item instanceof lazy.Op) {
        item.tosql(temp)
        return;
    }
    if (item instanceof lazy.SqlTable) {
        temp.push(mksqlfrag(item.fullname));
        return;
    }
    if (item instanceof lazy.Fragments) {
        temp.push(...item);
        return;
    }
    temp.push(mkvalfrag(item));
}
