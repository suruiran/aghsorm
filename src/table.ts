import { type Fragments, Frags, mksqlfrag, mkvalfrag } from "./frag.js";
import { lazy } from "./lazy.js";
import { IOpableItems, ITypedOpableItem, Op } from "./op.js";
import { quotetable, sql, type Value, type Identifier, DBContext } from "./types.js";
import { opItemToSQL } from "./utils.js";
import { type ITableDDL } from "./ddl.js";

export interface ISQLColumn {
    name: string;
    sqlname?: string;
    sqltype: string;
    nullable: boolean;
    isprimary: boolean;
    default: string | null;
    comment: string;
}

type WithOp<T> = {
    [K in keyof T]: ITypedOpableItem<T[K]> | null | undefined
};

type PartialRecord<T> = Partial<WithOp<T>>;

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

interface ITableOptions<T extends { [K in keyof T & string]: Value }> {
    schema: string;
    sqlschema?: string;
    name: string;
    sqlname?: string;
    fields: ISQLColumn[];
    ddl: ITableDDL<keyof T & string>;
}

export class SqlTable<T extends { [K in keyof T & string]: Value }> {
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
    private _fullname: string;
    /** @internal */
    private _ddl: ITableDDL<keyof T & string>;
    /** @internal */
    private _cxt: DBContext;

    constructor(ctx: DBContext, options: ITableOptions<T>) {
        this._cxt = ctx;
        this._schema = options.schema;
        this._sqlschema = options.sqlschema || this._schema;
        this._name = options.name;
        this._sqlname = options.sqlname || this._name;
        this._fields = options.fields;
        this._field_map = null;
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

    /** @internal */
    private quote_column_name(name: string): [string, ISQLColumn | null] {
        const fv = this.field_by_name(name as keyof T & string);
        if (fv) {
            return [this._cxt.quote("id", fv.sqlname || name), fv];
        }
        return [name, null];
    }

    get ddl(): ITableDDL<keyof T & string> {
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
            this._fullname = quotetable(this._cxt, this.schema, this.name);
        }
        return this._fullname;
    }

    column(key: keyof T & string, opts?: { fullname?: boolean }): Identifier {
        const field = this.field_by_name(key);
        if (!field) {
            throw new Error(`column ${key} not found on table ${this.name}`);
        }
        return new lazy.Identifier(
            field.sqlname || key,
            {
                table: this.name,
                fullname: opts?.fullname || false,
                ctx: this._cxt
            }
        );
    }

    colop(key: keyof T & string, opts?: { fullname?: boolean }): Op {
        return this.column(key, opts).op();
    }

    fakecol(name: string): keyof T & string {
        return name as keyof T & string;
    }

    /** @internal */
    private _expand_record(record: {
        [k: string]: IOpableItems;
    }): [string, IOpableItems, ISQLColumn | null][] {
        const pairs = Array.from(Object.entries(record)).filter(
            ([, v]) => typeof v !== "undefined"
        ).map(v => [...v, null]) as [string, IOpableItems, ISQLColumn | null][];
        if (pairs.length === 0) {
            throw new Error("empty record");
        }
        for (const pair of pairs) {
            const [qk, sf] = this.quote_column_name(pair[0]);
            pair[0] = qk;
            pair[2] = sf;
        }
        return pairs;
    }

    /** @internal */
    private _record_to_where_op(record: {
        [k: string]: IOpableItems;
    }): Op | null {
        let op: Op | null = null;
        for (const [key, value] of Object.entries(record)) {
            let eleop: Op;
            if (value instanceof Op) {
                const col = this.field_by_name(key as keyof T & string);
                value.__updatecol(this._cxt.quote("id", col?.sqlname || key));
                eleop = value;
            } else {
                if (value == null) {
                    eleop = this.column(key as any).op().isnull();
                } else {
                    const [qf, sf] = this.quote_column_name(key);
                    if (sf != null) {
                        eleop = new Op("EqualInWhereRecord", undefined, undefined, {
                            fmt(tmp) {
                                tmp.push(Frags.parenthesis.left);
                                tmp.push(mksqlfrag(qf));
                                tmp.push(mksqlfrag("="));
                                opItemToSQL(value, tmp, sf);
                                tmp.push(Frags.parenthesis.right);
                            },
                        })
                    } else {
                        eleop = this.column(key as any).op().eq(value as IOpableItems | Value);
                    }
                }
            }
            if (op) {
                op = op.and(eleop);
            } else {
                op = eleop;
            }
        }
        return op;
    }

    insert(record: PartialRecord<T>): Fragments {
        const pairs = this._expand_record(record as { [k: string]: IOpableItems });
        const tmp = new lazy.Fragments();
        tmp.push(mksqlfrag(`INSERT INTO ${this.fullname}`));
        tmp.push(Frags.parenthesis.left);

        const size = pairs.length;
        let idx = 0;
        for (const [key] of pairs) {
            tmp.push(mksqlfrag(key));
            idx++;
            if (idx < size) {
                tmp.push(Frags.comma);
            }
        }

        tmp.push(mksqlfrag(") VALUES ("));

        idx = 0;
        for (const [, item, sf] of pairs) {
            opItemToSQL(item, tmp, sf);
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
                throw new Error("Where clause is required");
            }
        }
        tmp.push(Frags.where);
        whereop.tosql(tmp);
    }

    /** @internal */
    private _push_opts(
        tmp: Fragments,
        opts?: IOrderOptions<T> &
            ILimitOptions &
            IOffsetOptions &
            IAllowEmptyWhereOptions
    ) {
        if (!opts) return;
        this.pushOrders(tmp, opts);
        this.pushLimitOffset(tmp, opts);
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
        const joinkind = opts?.joinkind || "AND";
        return new lazy.Op("TABLE.EQUALS", undefined, undefined, {
            fmt(tmp) {
                tmp.push(Frags.parenthesis.left);
                const size = pairs.length;
                let i = 0;
                for (const [k, v, f] of pairs) {
                    tmp.push(Frags.parenthesis.left);
                    tmp.push(mksqlfrag(k));

                    if (v == null) {
                        tmp.push(Frags.isnull);
                    } else {
                        tmp.push(Frags.equal);
                        opItemToSQL(v, tmp, f);
                    }

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
        where: PartialRecord<T> | Op,
        record: PartialRecord<T>,
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
        for (const [k, v, f] of pairs) {
            tmp.push(mksqlfrag(k));
            tmp.push(Frags.equal);
            opItemToSQL(v, tmp, f);
            idx++;
            if (idx < size) {
                tmp.push(Frags.comma);
            }
        }
        this._push_where(tmp, where, opts);
        this._push_opts(tmp, opts);
        return tmp;
    }

    /** @internal */
    private _push_groupby(tmp: Fragments, groupby: (string | Op)[]) {
        tmp.push(Frags.groupby);
        let idx = 0;
        const size = groupby.length;
        for (const k of groupby) {
            idx++;
            if (k instanceof Op) {
                k.tosql(tmp);
            } else {
                tmp.push(mksqlfrag(this.quote_column_name(k)[0]));
            }
            if (idx < size) {
                tmp.push(Frags.comma);
            }
        }
    }

    select(
        where: PartialRecord<T> | Op,
        opts?: {
            include?: ((keyof T & string) | Op)[];
            exclude?: (keyof T & string)[];
            groupby?: ((keyof T & string) | Op)[];
            having?: Op;
        } & IOrderOptions<T> &
            ILimitOptions &
            IOffsetOptions
    ): Fragments {
        const tmp = new lazy.Fragments();
        tmp.push(mksqlfrag(`SELECT`));

        if (
            opts &&
            ((opts.include && opts.include.length > 0) ||
                (opts.exclude && opts.exclude.length > 0))
        ) {
            let _keys = [] as ((keyof T & string) | Op)[];
            if (opts.include && opts.include.length > 0) {
                _keys = opts.include;
            } else {
                _keys = this._fields.map((v) => v.name) as any;
            }
            if (opts.exclude && opts.exclude.length > 0) {
                _keys = _keys.filter((v) => {
                    if (v instanceof Op) return true;
                    return !opts.exclude!.includes(v);
                });
            }
            if (_keys.length < 1) {
                throw new Error("empty keys");
            }
            let idx = 0;
            const size = _keys.length;
            for (const ele of _keys) {
                idx++;
                if (ele instanceof Op) {
                    ele.tosql(tmp);
                } else {
                    tmp.push(mksqlfrag(this.quote_column_name(ele)[0]));
                }
                if (idx < size) {
                    tmp.push(Frags.comma);
                }
            }
        } else {
            tmp.push(mksqlfrag("*"));
        }
        tmp.push(mksqlfrag(`FROM ${this.fullname}`));

        // where 
        this._push_where(tmp, where, { allowemptywhere: true });

        // group by, having
        if (opts?.groupby && opts.groupby.length > 0) {
            this._push_groupby(tmp, opts.groupby);

            if (opts?.having) {
                tmp.push(Frags.having);
                opts.having.tosql(tmp);
            }
        }

        // order by, limit, offset
        this._push_opts(tmp, opts);
        return tmp;
    }

    /** @internal */
    private pushOrders<T>(temp: Fragments, opts?: IOrderOptions<T>) {
        if (!opts || !opts.orderby) return;
        temp.push(Frags.orderby);
        const size = opts.orderby.length;
        let idx = 0;
        for (const item of opts.orderby) {
            if (typeof item === "string") {
                temp.push(mksqlfrag(this.quote_column_name(item)[0]));
            } else {
                temp.push(mksqlfrag(`${this.quote_column_name(item.field)} ${item.direction}`));
            }
            idx++;
            if (idx < size) {
                temp.push(Frags.comma);
            }
        }
    }

    /** @internal */
    private pushLimitOffset(
        temp: Fragments,
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
}

lazy.SqlTable = SqlTable;
