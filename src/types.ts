import { type IDBDDL } from "./ddl.js";
import { type ExportHandle, Fragments, type IExportOpts, mksqlfrag, type Batch } from "./frag.js";
import { DBCtxKey, lazy } from "./lazy.js";
import type { IOpableItems, Op } from "./op.js";
import { opItemToSQL } from "./utils.js";
import "zone.js";

export type Value =
    | string
    | number
    | boolean
    | Date
    | Uint8Array
    | bigint
    | null;

export interface DBContext extends IDBDDL {
    quote(id: string): string;
    register(fragments: Fragments, opts?: IExportOpts): void;
}

export function runInCtx(name: string, ctx: DBContext, fnc: () => any) {
    const scope = Zone.current.fork({ name, properties: { [DBCtxKey]: ctx } });
    return scope.run(fnc);
}

export function quotetable(dbctx: DBContext, scope: string | null, name: string): string {
    if (scope) {
        return `${dbctx.quote(scope)}.${dbctx.quote(name)}`;
    }
    return dbctx.quote(name);
}

export class Identifier {
    /** @internal */
    private _dbctx: DBContext | null;
    /** @internal */
    private _table: string | null;
    /** @internal */
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
        return new lazy.Op("", null, null, {
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

lazy.Identifier = Identifier;

export class RawSql {
    /** @internal */
    private _frags: Fragments;

    constructor(frags: Fragments) {
        this._frags = frags
    }

    op(): Op {
        return new lazy.Op("", null, null, {
            fmt: (tmp) => tmp.push(...this._frags),
        });
    }

    get frags(): Fragments {
        return this._frags;
    }

    export(opts?: IExportOpts): ExportHandle {
        return this._frags.export(opts);
    }
}

lazy.RawSql = RawSql;

export function sql(eles: TemplateStringsArray, ...exps: IOpableItems[]): RawSql {
    const tmp = new Fragments;
    for (let i = 0; i < eles.length; i++) {
        tmp.push(mksqlfrag(eles[i] as string));
        if (i < exps.length) {
            opItemToSQL(exps[i] as IOpableItems, tmp)
        }
    }
    return new RawSql(tmp);
}

export function rawsql(eles: TemplateStringsArray, ...exps: (Fragments | number | string | null)[]): Fragments {
    const tmp = new Fragments;
    for (let i = 0; i < eles.length; i++) {
        tmp.push(mksqlfrag(eles[i] as string));
        if (i < exps.length) {
            const ele = exps[i];
            if (ele === null) {
                tmp.push(mksqlfrag("NULL"));
                continue;
            }
            if (ele instanceof Fragments) {
                tmp.push(...ele);
                continue;
            }
            tmp.push(mksqlfrag(`${ele}`));
        }
    }
    return tmp;
}
