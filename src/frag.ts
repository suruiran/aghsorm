import type { DBContext, Value } from "./types.js";
import { lazy } from "./lazy.js";

export interface Fragment {
    sql?: string;
    value?: Value;
}

const fragsymbol = Symbol.for("frag");

function mark(v: Fragment): Fragment {
    Object.defineProperty(v, fragsymbol, { value: true, configurable: false, writable: false, enumerable: false })
    return v;
}

export function mksqlfrag(v: string): Fragment {
    return mark({ sql: v })
}

export function mkvalfrag(v: Value): Fragment {
    return mark({ value: v })
}

export function isfrag(obj: any): boolean {
    return Reflect.get(obj, fragsymbol) || false;
}

export const Frags = {
    comma: mksqlfrag(","),
    parenthesis: {
        left: mksqlfrag("("),
        right: mksqlfrag(")")
    },
    limit: mksqlfrag("LIMIT"),
    offset: mksqlfrag("OFFSET"),
    orderby: mksqlfrag("ORDER BY"),
    where: mksqlfrag("WHERE"),
    equal: mksqlfrag("="),
    set: mksqlfrag("SET"),
    between: mksqlfrag("BETWEEN"),
    and: mksqlfrag("AND")
}

export interface IExportOpts {
    label?: string;
    isquery?: boolean;
}

export class Fragments extends Array<Fragment> {
    constructor() {
        super();
    }

    export(dbctx: DBContext, opts?: IExportOpts): Fragments {
        dbctx.register(this, opts);
        return this;
    }

    push(...items: Fragment[]): number {
        for (const ele of items) {
            if (!isfrag(ele)) {
                throw new Error(`${ele} is not a fragment.`);
            }
        }
        return super.push(...items);
    }
}

lazy.Fragments = Fragments;
