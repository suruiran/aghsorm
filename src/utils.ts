import { Fragment, mksqlfrag, mkvalfrag, type Fragments } from "./frag.js";
import { lazy } from "./lazy.js";
import { type ISQLColumn } from "./table.js";

export function opItemToSQL(item: any, temp: Fragments, field?: ISQLColumn | null) {
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
    temp.push(mkvalfrag(item, field));
}

export function jointofrags<I>(temp: Fragments, iv: Iterable<I>, size: number, onele: (e: I) => Fragments, sep: Fragment) {
    let i = 0;
    for (const element of iv) {
        const fs = onele(element);
        temp.push(...fs);
        i++;
        if (i < size) {
            temp.push(sep);
        }
    }
}
