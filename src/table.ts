import { type Fragment, type Fragments, Frags, mksqlfrag, mkvalfrag } from "./frag.js";
import { lazy } from "./lazy.js";
import type { IOpableItems, ITypedOpableItem, Op } from "./op.js";
import { type DBContext, quotetable, sql, type Value } from "./types.js";
import { opItemToSQL } from "./utils.js";

export interface ISQLColumn {
    name: string;
    sqlname?: string;
    sqltype: string;
    nullable: boolean;
    isprimary: boolean;
    default: string | null;
    comment: string;
}

export interface ISQLIndex {
    name: string;
    unique?: boolean;
    fields: string[];
}

type ExtractFromKeys<T, K extends readonly (keyof T & string)[]> = Pick<T, K[number]>;
type ExtractNoInKeys<T, K extends readonly (keyof T & string)[]> = Omit<T, K[number]>;
type WithOp<T, Undefinedable extends boolean = false> = {
    [K in keyof T]: Undefinedable extends true
    ? ITypedOpableItem<T[K]> | null | undefined
    : ITypedOpableItem<T[K]> | null;
};

// prettier-ignore
type InsertRecord<T, PKS extends readonly (keyof T & string)[]> =
    WithOp<Required<ExtractFromKeys<T, PKS>>>
    &
    WithOp<Partial<ExtractNoInKeys<T, PKS>>, true>;

type PartialRecord<T> = Partial<WithOp<T, true>>;

type IOrder<T> =
    | {
        field: keyof T & string;
        direction: "ASC" | "DESC";
    }
    | (keyof T & string);

const TrueOp = sql`(1 = 1)`.op();

export interface IOrderOptions<T> {
    orderby?: IOrder<T>[];
}

export interface ILimitOptions {
    limit?: number;
}

export interface IOffsetOptions {
    offset?: number;
}

interface IAllowEmptyWhereOptions {
    allowemptywhere?: boolean;
}

export interface IDDLImpl<Key> {
    newcol(newcol: ISQLColumn): Fragments;
    dropcol(col: Key): Fragments;
    modcol(from: Key, to: ISQLColumn): Fragments;
    dropindex(index: string): Fragments;
    createindex(index: ISQLIndex): Fragments;
}

interface ITableOptions<T extends { [K in keyof T & string]: Value }> {
    dbctx: DBContext;
    schema: string;
    sqlschema?: string;
    name: string;
    sqlname?: string;
    fields: ISQLColumn[];
    indexes: ISQLIndex[];
    ddl: IDDLImpl<keyof T & string>;
}

export class SqlTable<
    T extends { [K in keyof T & string]: Value },
    PKS extends readonly (keyof T & string)[]
> {
    /** @internal */
    private _schema: string;
    /** @internal */
    private _sqlschema: string;
    /** @internal */
    private _name: string;
    /** @internal */
    private _sqlname: string;
    /** @internal */
    /** @internal */
    private _fields: ISQLColumn[];
    /** @internal */
    private _field_map: Map<string, ISQLColumn> | null;
    /** @internal */
    private _indexes: ISQLIndex[];
    /** @internal */
    private _fullname: string;
    /** @internal */
    private _dbctx: DBContext;
    /** @internal */
    private _ddl: IDDLImpl<keyof T & string>;

    constructor(options: ITableOptions<T>) {
        this._dbctx = options.dbctx;
        this._schema = options.schema;
        this._sqlschema = options.sqlschema || this._schema;
        this._name = options.name;
        this._sqlname = options.sqlname || this._name;
        this._fields = options.fields;
        this._field_map = null;
        this._indexes = options.indexes;
        this._ddl = options.ddl;

        this._fullname = "";
        if (this._fields.length > 12) {
            this._field_map = new Map(this._fields.map((f) => [f.name, f]));
        }
    }

    /** @internal */
    private field_by_name(key: keyof T & string): ISQLColumn | null {
        if (this._field_map) {
            return this._field_map.get(key) || null;
        }
        return this._fields.find((f) => f.name === key) || null;
    }

    get ddl(): IDDLImpl<keyof T & string> {
        return this._ddl;
    }

    get schema(): string {
        return this._sqlschema || this._schema;
    }

    get name(): string {
        return this._sqlname || this._name;
    }

    get fullname(): string {
        if (!this._fullname) {
            this._fullname = quotetable(this._dbctx, this.schema, this.name);
        }
        return this._fullname;
    }

    field(key: keyof T & string): Op {
        const field = this.field_by_name(key);
        if (!field) {
            return new lazy.Identifier(key).op();
        }
        return new lazy.Identifier(field.sqlname || key, { dbctx: this._dbctx, table: this.name }).op();
    }

    /** @internal */
    private _expand_record(record: {
        [k: string]: IOpableItems;
    }): [string, IOpableItems][] {
        const pairs = Array.from(Object.entries(record)).filter(
            ([, v]) => typeof v !== "undefined"
        );
        if (pairs.length === 0) {
            throw new Error("empty record");
        }
        for (const pair of pairs) {
            const key = pair[0];
            const field = this.field_by_name(key as keyof T & string);
            if (field) {
                pair[1] = field.sqlname || key;
            }
        }
        return pairs;
    }

    /** @internal */
    private _record_to_where_op(record: {
        [k: string]: IOpableItems;
    }): Op | null {
        let op: Op | null = null;
        for (const [key, value] of Object.entries(record)) {
            const _op = this.field(key as any).eq(value as IOpableItems | Value);
            if (op) {
                op = op.and(_op);
            } else {
                op = _op;
            }
        }
        return op;
    }

    insert(record: InsertRecord<T, PKS>): Fragments {
        const pairs = this._expand_record(record);
        const tmp = new lazy.Fragments();
        tmp.push(mksqlfrag(`INSERT INTO ${this.fullname}`));
        tmp.push(Frags.parenthesis.left);

        const size = pairs.length;
        let idx = 0;
        for (const [key] of pairs) {
            tmp.push(mksqlfrag(this._dbctx.quote(key)));
            idx++;
            if (idx < size) {
                tmp.push(Frags.comma);
            }
        }

        tmp.push(mksqlfrag(") VALUES ("));

        idx = 0;
        for (const [, item] of pairs) {
            opItemToSQL(item, tmp);
            idx++;
            if (idx < size) {
                tmp.push(Frags.comma);
            }
        }
        tmp.push(Frags.parenthesis.right);
        return tmp;
    }

    /** @internal */
    private _push_where(
        tmp: Fragments,
        where: PartialRecord<T> | Op,
        opts?: IAllowEmptyWhereOptions
    ) {
        let whereop: Op | null = null;
        if (where instanceof lazy.Op) {
            whereop = where;
        } else {
            whereop = this._record_to_where_op(
                where as { [k: string]: IOpableItems }
            );
        }
        if (!whereop) {
            if (opts?.allowemptywhere) {
                whereop = TrueOp;
            } else {
                throw new Error("Where clause is required for delete");
            }
        }
        tmp.push(Frags.where);
        whereop.tosql(tmp);
    }

    /** @internal */
    private _push_opts(
        tmp: Fragment[],
        opts?: IOrderOptions<T> &
            ILimitOptions &
            IOffsetOptions &
            IAllowEmptyWhereOptions
    ) {
        if (!opts) return;
        pushOrders(this._dbctx, tmp, opts);
        pushLimitOffset(tmp, opts);
    }

    delete(
        where: PartialRecord<T> | Op,
        opts?: {
            orderby?: IOrder<T>[];
            limit?: number;
            offset?: number;
            allowemptywhere?: boolean;
        }
    ): Fragments {
        const tmp = new lazy.Fragments();
        tmp.push(mksqlfrag(`DELETE FROM ${this.fullname}`));
        this._push_where(tmp, where, opts);
        this._push_opts(tmp, opts);
        return tmp;
    }

    equals(record: PartialRecord<T>, opts?: { joinkind?: "AND" | "OR" }): Op {
        const pairs = this._expand_record(record as any);
        const dbctx = this._dbctx;
        const joinkind = opts?.joinkind || "AND";
        return new lazy.Op("", undefined, undefined, {
            fmt(tmp) {
                tmp.push(Frags.parenthesis.left);
                const size = pairs.length;
                let i = 0;
                for (const [k, v] of pairs) {
                    tmp.push(Frags.parenthesis.left);
                    tmp.push(mksqlfrag(dbctx.quote(k)));
                    tmp.push(Frags.equal);
                    opItemToSQL(v, tmp);
                    tmp.push(Frags.parenthesis.right);
                    i++;
                    if (i < size) {
                        tmp.push(mksqlfrag(joinkind));
                    }
                }
                tmp.push(Frags.parenthesis.right);
            },
        });
    }

    update(
        record: PartialRecord<T>,
        where: PartialRecord<T> | Op,
        opts?: IOrderOptions<T> &
            ILimitOptions &
            IOffsetOptions &
            IAllowEmptyWhereOptions
    ): Fragments {
        const pairs = this._expand_record(record as any);
        const tmp = new lazy.Fragments();
        tmp.push(mksqlfrag(`UPDATE ${this.fullname}`));
        tmp.push(Frags.set);

        const size = pairs.length;
        let idx = 0;
        for (const [k, v] of pairs) {
            tmp.push(mksqlfrag(this._dbctx.quote(k)));
            tmp.push(Frags.equal);
            opItemToSQL(v, tmp);
            idx++;
            if (idx < size) {
                tmp.push(Frags.comma);
            }
        }
        this._push_where(tmp, where, opts);
        this._push_opts(tmp, opts);
        return tmp;
    }

    select(
        where: PartialRecord<T> | Op,
        opts?: {
            include?: (keyof T & string)[];
            exclude?: (keyof T & string)[];
            groupby?: (keyof T & string)[];
        } & IOrderOptions<T> &
            ILimitOptions &
            IOffsetOptions
    ): Fragments {
        let keys = "*";
        if (
            opts &&
            ((opts.include && opts.include.length > 0) ||
                (opts.exclude && opts.exclude.length > 0))
        ) {
            let _keys = [] as (keyof T & string)[];
            if (opts.include && opts.include.length > 0) {
                _keys = opts.include;
            } else {
                _keys = this._fields.map((v) => v.name) as any;
            }
            if (opts.exclude && opts.exclude.length > 0) {
                _keys = _keys.filter((v) => !opts.exclude!.includes(v));
            }
            _keys = _keys.map((v) => this._dbctx.quote(v)) as any;
            keys = _keys.join(", ");
        }
        const tmp = new lazy.Fragments();
        tmp.push(mksqlfrag(`SELECT ${keys} FROM ${this.fullname}`));
        this._push_where(tmp, where, { allowemptywhere: true });
        this._push_opts(tmp, opts);
        return tmp;
    }
}

lazy.SqlTable = SqlTable;

export function pushOrders<T>(dbctx: DBContext, temp: Fragment[], opts?: IOrderOptions<T>) {
    if (!opts || !opts.orderby) return;
    temp.push(Frags.orderby);
    const size = opts.orderby.length;
    let idx = 0;
    for (const item of opts.orderby) {
        if (typeof item === "string") {
            temp.push(mksqlfrag(dbctx.quote(item)));
        } else {
            temp.push(mksqlfrag(`${dbctx.quote(item.field)} ${item.direction}`));
        }
        idx++;
        if (idx < size) {
            temp.push(Frags.comma);
        }
    }
}

export function pushLimitOffset(
    temp: Fragment[],
    opts?: ILimitOptions & IOffsetOptions
) {
    if (!opts) return;
    if (opts.limit != null) {
        temp.push(Frags.limit);
        temp.push(mkvalfrag(opts.limit));
    }
    if (opts.offset != null) {
        temp.push(Frags.offset);
        temp.push(mkvalfrag(opts.offset));
    }
}