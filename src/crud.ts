import { DDLGenerator } from "./ddl.js";
import { Fragment, Fragments, Frags, mksqlfrag, mkvalfrag } from "./frag.js";
import { IOpableItems, ITypedOpableItem, Op } from "./op.js";
import { Identifier, quote, sql, Value } from "./types.js";
import { opItemToSQL } from "./utils.js";

export interface ISQLColumn {
    name: string;
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

type ExtractFromKeys<T, K extends readonly (keyof T)[]> = Pick<T, K[number]>;
type ExtractNoInKeys<T, K extends readonly (keyof T)[]> = Omit<T, K[number]>;
type WithOp<T, Undefinedable extends boolean = false> = {
    [K in keyof T]: Undefinedable extends true
    ? ITypedOpableItem<T[K]> | null | undefined
    : ITypedOpableItem<T[K]> | null;
};

// prettier-ignore
type InsertRecord<T, PKS extends readonly (keyof T)[]> =
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

export class SqlTable<
    T extends { [K in keyof T]: Value },
    PKS extends readonly (keyof T)[]
> {
    private _schema: string;
    private _name: string;
    private _fields: ISQLColumn[];
    private _indexes: ISQLIndex[];
    private _ddl: DDLGenerator<keyof T>;

    constructor(schema: string, name: string, fields: ISQLColumn[], indexes: ISQLIndex[]) {
        this._schema = schema;
        this._name = name;
        this._fields = fields;
        this._indexes = indexes;

        this._ddl = new DDLGenerator(this);
    }

    get ddl(): DDLGenerator<keyof T> {
        return this._ddl;
    }

    field(key: keyof T & string): Op {
        return new Identifier(key, this._name).op();
    }

    private _expand_record(record: {
        [k: string]: IOpableItems;
    }): [string, IOpableItems][] {
        const pairs = Array.from(Object.entries(record)).filter(
            ([, v]) => typeof v !== "undefined"
        );
        if (pairs.length === 0) {
            throw new Error("empty record");
        }
        return pairs;
    }

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
        let tablename = quote(this._schema, this._name);
        const pairs = this._expand_record(record);
        const tmp = new Fragments();
        tmp.push(mksqlfrag(`INSERT INTO ${tablename}`));
        tmp.push(Frags.parenthesis.left);

        const size = pairs.length;
        let idx = 0;
        for (const [key] of pairs) {
            tmp.push(mksqlfrag(dbctx.quote(key)));
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

    _push_where(
        tmp: Fragments,
        where: PartialRecord<T> | Op,
        opts?: IAllowEmptyWhereOptions
    ) {
        let whereop: Op | null = null;
        if (where instanceof Op) {
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

    _push_opts(
        tmp: Fragment[],
        opts?: IOrderOptions<T> &
            ILimitOptions &
            IOffsetOptions &
            IAllowEmptyWhereOptions
    ) {
        if (!opts) return;
        pushOrders(tmp, opts);
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
        let tablename = quote(this._schema, this._name);
        const tmp = new Fragments();
        tmp.push(mksqlfrag(`DELETE FROM ${tablename}`));
        this._push_where(tmp, where, opts);
        this._push_opts(tmp, opts);
        return tmp;
    }

    equals(record: PartialRecord<T>, opts?: { joinkind?: "AND" | "OR" }): Op {
        const pairs = this._expand_record(record as any);
        const joinkind = opts?.joinkind || "AND";
        return new Op("", undefined, undefined, {
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
        const tablename = quote(this._schema, this._name);
        const pairs = this._expand_record(record as any);
        const tmp = new Fragments();
        tmp.push(mksqlfrag(`UPDATE ${tablename}`));
        tmp.push(Frags.set);

        const size = pairs.length;
        let idx = 0;
        for (const [k, v] of pairs) {
            tmp.push(mksqlfrag(dbctx.quote(k)));
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
        const tablename = quote(this._schema, this._name);

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
            _keys = _keys.map((v) => dbctx.quote(v)) as any;
            keys = _keys.join(", ");
        }
        const tmp = new Fragments();
        tmp.push(mksqlfrag(`SELECT ${keys} FROM ${tablename}`));
        this._push_where(tmp, where, { allowemptywhere: true });
        this._push_opts(tmp, opts);
        return tmp;
    }
}

export function pushOrders<T>(temp: Fragment[], opts?: IOrderOptions<T>) {
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