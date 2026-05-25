import { type IDBDDL } from "./ddl.js";
import { type ExportHandle, type Fragments, type IExportOpts, mksqlfrag } from "./frag.js";
import { lazy } from "./lazy.js";
import type { IOpableItems, Op } from "./op.js";
import { opItemToSQL } from "./utils.js";

export type Value =
    | string
    | number
    | boolean
    | Date
    | Uint8Array
    | bigint
    | null;

export interface DBContext extends IDBDDL {
    quote(kind: "id" | "stringliteral", txt: string): string;
    register(fragments: Fragments, opts?: IExportOpts): void;
}

export function quotetable(dbctx: DBContext, scope: string | null, name: string): string {
    if (scope) {
        return `${dbctx.quote("id", scope)}.${dbctx.quote("id", name)}`;
    }
    return dbctx.quote("id", name);
}

export class Identifier {
    /** @internal */
    private _table: string | null;
    /** @internal */
    private _name: string;
    /** @internal */
    private _fullname: boolean | null;
    /** @internal */
    private _ctx: DBContext | null;

    constructor(
        name: string,
        opts?: {
            table?: string,
            fullname?: boolean,
            ctx?: DBContext,
        }
    ) {
        this._ctx = opts?.ctx || null;
        this._name = name;
        this._fullname = opts?.fullname || false;
        this._table = opts?.table || null;
    }

    string(opts?: { fullname?: boolean }) {
        const fullname = opts?.fullname || this._fullname;
        const ctx = this._ctx;
        if (!ctx) {
            if (this._table && fullname) {
                return `${this._table}.${this._name}`;
            }
            return this._name;
        }
        if (fullname) {
            return quotetable(ctx, this._table, this._name)
        }
        return ctx.quote("id", this._name);
    }


    op(): Op {
        return new lazy.Op("IDENTIFIER", undefined, undefined, {
            fmt: (tmp) => {
                tmp.push(mksqlfrag(this.string()));
            },
        });
    }
}

lazy.Identifier = Identifier;

export class RawSql {
    /** @internal */
    private _frags: Fragments;

    constructor(frags: Fragments) {
        this._frags = frags
    }

    op(): Op {
        return new lazy.Op("RAWSQL", undefined, undefined, {
            fmt: (tmp) => tmp.push(...this._frags),
        });
    }

    get frags(): Fragments {
        return this._frags;
    }

    export(ctx: DBContext, opts?: IExportOpts): ExportHandle {
        return this._frags.export(ctx, opts);
    }
}

lazy.RawSql = RawSql;

export function sql(eles: TemplateStringsArray, ...exps: any[]): RawSql {
    const tmp = new lazy.Fragments;
    for (let i = 0; i < eles.length; i++) {
        tmp.push(mksqlfrag(eles[i] as string));
        if (i < exps.length) {
            opItemToSQL(exps[i] as IOpableItems, tmp);
        }
    }
    return new RawSql(tmp);
}

export function rawsql(eles: TemplateStringsArray, ...exps: (Fragments | bigint | number | string | null)[]): Fragments {
    const tmp = new lazy.Fragments;
    for (let i = 0; i < eles.length; i++) {
        tmp.push(mksqlfrag(eles[i] as string));
        if (i < exps.length) {
            const ele = exps[i];
            if (ele === null) {
                tmp.push(mksqlfrag("NULL"));
                continue;
            }
            if (ele instanceof lazy.Fragments) {
                tmp.push(...ele);
                continue;
            }
            tmp.push(mksqlfrag(`${ele}`));
        }
    }
    return tmp;
}
