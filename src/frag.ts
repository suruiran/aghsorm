import { type Value } from "./types.js";
import { type Op } from "./op.js";

import { lazy } from "./lazy.js";
import { mustdbctx } from "./ctxvals.js";

export interface Fragment {
    sql?: string;
    value?: Value;
}

const fragsymbol = Symbol.for("frag");

function mark(v: Fragment): Fragment {
    Object.defineProperty(v, fragsymbol, { value: true, configurable: false, writable: false, enumerable: false });
    return v;
}

export function mksqlfrag(v: string): Fragment {
    return mark({ sql: v })
}

export function mkvalfrag(v: Value): Fragment {
    return mark({ value: v })
}

export function isfrag(obj: any): boolean {
    return Reflect.get(obj, fragsymbol) === true;
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
    and: mksqlfrag("AND"),
    isnull: mksqlfrag("IS NULL"),
    groupby: mksqlfrag("GROUP BY"),
    having: mksqlfrag("HAVING"),
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

export interface IHexColRenderOpts {
    lowercase?: boolean;
    sep?: string;
    linewidth?: number;
}

export interface IBitmapColRenderOpts {
    sep?: string;
    linewidth?: number;
}

export const ColRendererKinds = ["datetime", "boolean", "enum", "string", "uuid", "hex", "bitmap"] as const;
export type ColRendererKind = (typeof ColRendererKinds)[number];

export class ExportHandle {
    /** @internal */
    private _opts: IExportOpts;

    constructor(opts: IExportOpts) {
        this._opts = opts;
    }

    label(label: string): ExportHandle {
        this._opts.label = label;
        return this;
    }

    isquery(isquery: boolean = true): ExportHandle {
        this._opts.isquery = isquery;
        return this;
    }

    get variants(): VariantsHandle {
        return new VariantsHandle(this._opts);
    }
}

export class VariantsHandle {
    /** @internal */
    private _opts: IExportOpts;
    /** @internal */
    constructor(opts: IExportOpts) {
        this._opts = opts;
    }

    /** @internal */
    private colrender(colnames: string | string[], kind: ColRendererKind, opts?: any) {
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
                case "hex": {
                    const tmp = opts as IHexColRenderOpts;
                    if (tmp.lowercase) record.lowercase = tmp.lowercase.toString();
                    if (tmp.sep) record.sep = tmp.sep;
                    if (tmp.linewidth) record.linewidth = tmp.linewidth.toString();
                    break;
                }
                case "bitmap": {
                    const tmp = opts as IBitmapColRenderOpts;
                    if (tmp.sep) record.sep = tmp.sep;
                    if (tmp.linewidth) record.linewidth = tmp.linewidth.toString();
                    break;
                }
                default: {
                    throw new Error(`Unsupported column renderer kind: ${kind}`);
                }
            }
        }
        if (!this._opts.colrenderers) this._opts.colrenderers = {};

        const _cols = [] as string[];
        if (typeof colnames === "string") {
            _cols.push(colnames);
        } else {
            _cols.push(...colnames);
        }
        for (const _col of _cols) {
            this._opts.colrenderers[_col] = { kind, opts: record };
        }
    }

    datetime(
        cols: string | string[],
        opts?: {
            unit?: "auto" | "sec" | "mills" | "nano";
            layout?: string;
            tz?: string;
        }
    ): this {
        this.colrender(cols, "datetime", opts);
        return this;
    }

    enum(cols: string | string[], opts?: Iterable<[string, number]>): this {
        this.colrender(cols, "enum", opts);
        return this;
    }

    string(cols: string | string[], opts?: { encoding?: string; }): this {
        this.colrender(cols, "string", opts);
        return this;
    }

    uuid(col: string | string[]): this {
        this.colrender(col, "uuid");
        return this;
    }


    boolean(col: string | string[]): this {
        this.colrender(col, "boolean");
        return this;
    }

    hex(
        col: string | string[],
        opts?: {
            lowercase?: boolean;
            sep?: string;
            linewidth?: number;
        }
    ): this {
        this.colrender(col, "hex", opts);
        return this;
    }

    bitmap(
        col: string | string[],
        opts?: {
            sep?: string;
            linewidth?: number;
        }
    ): this {
        this.colrender(col, "bitmap", opts);
        return this;
    }
}

export class Fragments {
    /** @internal */
    _items: Fragment[];

    constructor() {
        this._items = [];
    }

    *[Symbol.iterator]() {
        for (const element of this._items) {
            yield element;
        }
    }

    get length(): number {
        return this._items.length;
    }

    export(opts?: IExportOpts): ExportHandle {
        const _opts = opts || {};
        const handle = new ExportHandle(_opts);
        const dbctx = mustdbctx();
        dbctx.register(this, _opts);
        return handle;
    }

    push(...items: Fragment[]): number {
        for (const ele of items) {
            if (!isfrag(ele)) {
                throw new Error(`${ele} is not a fragment.`);
            }
        }
        return this._items.push(...items);
    }

    op(): Op {
        return new lazy.Op("FRAGMENTS", undefined, undefined, {
            fmt: (tmp) => tmp.push(...this),
        });
    }
}

lazy.Fragments = Fragments;
