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

export interface IColRendererOpts {
    kind: ColRendererKind;
    opts?: Record<string, string>;
}

export interface IExportOpts {
    label?: string;
    isquery?: boolean;
    colrenderers?: Record<string, IColRendererOpts>;
}

export interface IDatetimeColRendererOpts {
    unit?: "auto" | "sec" | "mills" | "nano";
    layout?: string;
    tz?: string;
}

export interface ITxtColRenderOpts {
    encoding?: string;
}

export const ColRendererKinds = ["datetime", "boolean", "enum", "string", "uuid"] as const;
export type ColRendererKind = (typeof ColRendererKinds)[number];

export class ExportHandle {
    /** @internal */
    private _opts: IExportOpts;

    constructor(opts: IExportOpts) {
        this._opts = opts;
    }

    lable(lable: string): ExportHandle {
        this._opts.label = lable;
        return this;
    }

    isquery(isquery: boolean = true): ExportHandle {
        this._opts.isquery = isquery;
        return this;
    }

    colrender(colname: string, kind: "datetime", opts?: IDatetimeColRendererOpts): ExportHandle;
    colrender(colname: string, kind: "enum", items: Iterable<[string, number]>): ExportHandle;
    colrender(colname: string, kind: "string", opts?: ITxtColRenderOpts): ExportHandle;
    colrender(colname: string, kind: "uuid"): ExportHandle;
    colrender(colname: string, kind: "boolean"): ExportHandle;
    colrender(colname: string, kind: ColRendererKind, opts?: any): ExportHandle {
        let record = {} as Record<string, string>;
        if (opts) {
            switch (kind) {
                case "string": {
                    const tmp = opts as ITxtColRenderOpts;
                    if (tmp.encoding) record.encoding = tmp.encoding;
                    break;
                }
                case "datetime": {
                    const tmp = opts as IDatetimeColRendererOpts;
                    if (tmp.unit) record.unit = tmp.unit;
                    if (tmp.layout) record.layout = tmp.layout;
                    if (tmp.tz) record.tz = tmp.tz;
                    break;
                }
                case "enum": {
                    const tmp = opts as Iterable<[string, number]>;
                    for (const [k, v] of tmp) {
                        record[k] = `${v}`;
                    }
                    break;
                }
                default: {
                    throw new Error(`Unsupported column renderer kind: ${kind}`);
                }
            }
        }
        if (!this._opts.colrenderers) this._opts.colrenderers = {};
        this._opts.colrenderers[colname] = { kind, opts: record };
        return this;
    }
}

export class Fragments extends Array<Fragment> {
    constructor() {
        super();
    }

    export(dbctx: DBContext, opts?: IExportOpts): ExportHandle {
        const _opts = opts || {};
        const handle = new ExportHandle(_opts);
        dbctx.register(this, _opts);
        return handle;
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
