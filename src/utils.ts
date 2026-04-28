import { mksqlfrag, mkvalfrag, type Fragments } from "./frag.js";
import { lazy } from "./lazy.js";
import type { IOpableItems } from "./op.js";

export function opItemToSQL(item: IOpableItems, temp: Fragments) {
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


export function QuoteSQLStringLiteral(v: string) {
    if (v.includes("\\")) {
        throw new Error("aghsorm: string literal cannot contain backslash");
    }
    return `'${v.replaceAll("'", "''")}'`;
}
