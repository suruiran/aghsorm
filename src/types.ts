import { Fragments, type IExportOpts, mksqlfrag } from "./frag.js";
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

export function quotetable(dbctx: DBContext, scope: string | null, name: string): string {
    if (scope) {
        return `${dbctx.quote(scope)}.${dbctx.quote(name)}`;
    }
    return dbctx.quote(name);
}

export class Identifier {
    private _dbctx: DBContext | null;
    private _table: string | null;
    private _name: string;
    constructor(name: string, opts?: {
        dbctx?: DBContext,
        table?: string,
    }) {
        this._dbctx = opts?.dbctx || null;
        this._name = name;
        this._table = opts?.table || null;
    }

    op(): Op {
        return new Op("", null, null, {
            fmt: (tmp) => {
                if (!this._dbctx) {
                    if (this._table) {
                        tmp.push(mksqlfrag(`${this._table}.${this._name}`));
                        return;
                    }
                    tmp.push(mksqlfrag(this._name));
                    return;
                }
                tmp.push(mksqlfrag(quotetable(this._dbctx, this._table, this._name)));
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

    export(dbctx: DBContext, opts?: IExportOpts) {
        this._frags.export(dbctx, opts);
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
