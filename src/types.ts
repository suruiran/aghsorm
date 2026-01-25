import { Fragments, IExportOpts, mksqlfrag } from "./frag.js";
import { Op } from "./op.js";
import { opItemToSQL } from "./utils.js";

export type Value =
    | string
    | number
    | boolean
    | Date
    | Uint8Array
    | bigint
    | null;


export interface DBContext {
    quote(id: string): string;
    register(fragments: Fragments, opts?: IExportOpts): void;
}

export function quote(scope: string | null, name: string): string {
    if (scope) {
        return `${dbctx.quote(scope)}.${dbctx.quote(name)}`;
    }
    return dbctx.quote(name);
}

declare global {
    var dbctx: DBContext;
}

export class Identifier {
    private _table: string | null;
    private _name: string;
    constructor(key: string, table?: string) {
        this._name = key;
        this._table = table || null;
    }

    op(): Op {
        return new Op("", null, null, {
            fmt: (tmp) => {
                tmp.push(mksqlfrag(quote(this._table, this._name)));
            },
        });
    }
}

export class RawSql {
    private _frags: Fragments;

    constructor(frags: Fragments) {
        this._frags = frags
    }

    op(): Op {
        return new Op("", null, null, {
            fmt: (tmp) => tmp.push(...this._frags),
        });
    }

    get frags(): Fragments {
        return this._frags;
    }

    export(opts?: IExportOpts) {
        this._frags.export(opts);
    }
}

export function sql(eles: TemplateStringsArray, ...exps: any[]): RawSql {
    const tmp = new Fragments;
    for (let i = 0; i < eles.length; i++) {
        tmp.push(mksqlfrag(eles[i] as string));
        if (exps[i] != null) {
            opItemToSQL(exps[i], tmp)
        }
    }
    return new RawSql(tmp);
}

export function op(eles: TemplateStringsArray, ...exps: any[]): Op {
    return sql(eles, ...exps).op();
}
