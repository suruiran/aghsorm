import { Op } from "./op.js";
import { opItemToSQL, pushLimitOffset, pushOrders } from "./utils.js";
export { Op } from "./op.js";

export type Value =
  | string
  | number
  | boolean
  | Date
  | Uint8Array
  | bigint
  | null;

export interface IColumn {
  name: string;
  sqltype: string;
  nullable: boolean;
  isprimary: boolean;
  default: NullString;
  comment: string;
}
export interface DBContext {
  quote(table: string | null, column: string | null): string;
  render: (fragments: Fragment[]) => [string, Value[]];
  checkfuncname?: (fn: string) => boolean;
}

declare global {
  var dbctx: DBContext;
}

export class Identifier {
  _table: string | null;
  _name: string;
  constructor(key: string, table?: string) {
    this._name = key;
    this._table = table || null;
  }

  op(): Op {
    return new Op("", null, null, {
      fmt: (tmp) => {
        tmp.push({ sql: dbctx.quote(this._table, this._name) });
      },
    });
  }
}

export class RawSql {
  _sql: string;

  constructor(sql: string) {
    this._sql = sql;
  }

  op(): Op {
    return new Op("", null, null, {
      fmt: (tmp) => tmp.push({ sql: this._sql }),
    });
  }
}

export function rawsql(eles: TemplateStringsArray, ...exps: any[]) {
  const tmp = [] as string[];
  for (let i = 0; i < eles.length; i++) {
    tmp.push(eles[i] as string);
    if (exps[i] != null) {
      tmp.push(`${exps[i]}`);
    }
  }
  return new RawSql(tmp.join(""));
}

export type IOpableItems = Value | Identifier | RawSql | Op;
export type ITypedOpableItem<T> = T | Identifier | RawSql | Op;

export interface Fragment {
  sql?: string;
  value?: Value;
}


interface NullString {
  String: string;
  Valid: boolean;
}

interface IIndex {
  name: string;
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

export class DDLGenerator<Key> {
  private _table: Table<any, any>;

  constructor(table: Table<any, any>) {
    this._table = table;
  }

  newcol(newcol: IColumn) { }
  dropcol(col: Key) { }
  modcol(fromcol: Key, tocol: IColumn): void { }
  softdrop() { }
  drop() { }
}

const TrueOp = rawsql`(1 = 1)`.op();

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

export class Table<
  T extends { [K in keyof T]: Value },
  PKS extends readonly (keyof T)[]
> {
  private _name: string;
  private _fields: IColumn[];
  private _indexes: IIndex[];
  private _ddl: DDLGenerator<keyof T>;

  constructor(name: string, fields: IColumn[], indexes: IIndex[]) {
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

  insert(record: InsertRecord<T, PKS>): Fragment[] {
    let tablename = dbctx.quote(this._name, null);
    const pairs = this._expand_record(record);
    const tmp = [] as Fragment[];
    tmp.push({ sql: `INSERT INTO ${tablename}` });
    tmp.push({ sql: "(" });

    const size = pairs.length;
    let idx = 0;
    for (const [key,] of pairs) {
      tmp.push({ sql: dbctx.quote(null, key) });
      idx++;
      if (idx < size) {
        tmp.push({ sql: "," });
      }
    }

    tmp.push({ sql: ") VALUES (" });

    idx = 0;
    for (const [, item] of pairs) {
      opItemToSQL(item, tmp);
      idx++;
      if (idx < size) {
        tmp.push({ sql: "," })
      }
    }
    tmp.push({ sql: ")" });
    return tmp;
  }

  _push_where(
    tmp: Fragment[],
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
    tmp.push({ sql: "WHERE" });
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
  ): Fragment[] {
    let tablename = dbctx.quote(this._name, null);
    const tmp = [{ sql: `DELETE FROM ${tablename}` }] as Fragment[];
    this._push_where(tmp, where, opts);
    this._push_opts(tmp, opts);
    return tmp;
  }

  equals(record: PartialRecord<T>, opts?: { joinkind?: "AND" | "OR" }): Op {
    const pairs = this._expand_record(record as any);
    const joinkind = opts?.joinkind || "AND";
    return new Op("", undefined, undefined, {
      fmt(tmp) {
        tmp.push({ sql: "(" })
        const size = pairs.length;
        let i = 0;
        for (const [k, v] of pairs) {
          tmp.push({ sql: "(" })
          tmp.push({ sql: dbctx.quote(null, k) });
          tmp.push({ sql: "=" });
          opItemToSQL(v, tmp);
          tmp.push({ sql: ")" })
          i++;
          if (i < size) {
            tmp.push({ sql: joinkind })
          }
        }
        tmp.push({ sql: ")" })
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
  ): Fragment[] {
    const tablename = dbctx.quote(this._name, null);
    const pairs = this._expand_record(record as any);
    const tmp = [{ sql: `UPDATE ${tablename}` }] as Fragment[];
    tmp.push({ sql: "SET" });

    const size = pairs.length;
    let idx = 0;
    for (const [k, v] of pairs) {
      tmp.push({ sql: dbctx.quote(null, k) });
      tmp.push({ sql: "=" });
      opItemToSQL(v, tmp);
      idx++;
      if (idx < size) {
        tmp.push({ sql: "," });
      }
    }
    this._push_where(tmp, where, opts);
    this._push_opts(tmp, opts);
    return tmp;
  }

  select(
    where: PartialRecord<T> | Op,
    opts?: {
      include?: ((keyof T) & string)[];
      exclude?: ((keyof T) & string)[];
      groupby?: ((keyof T) & string)[];
    } & IOrderOptions<T> &
      ILimitOptions &
      IOffsetOptions
  ): Fragment[] {
    const tablename = dbctx.quote(this._name, null);

    let keys = "*";
    if (opts && ((opts.include && opts.include.length > 0) || (opts.exclude && opts.exclude.length > 0))) {
      let _keys = [] as ((keyof T) & string)[];
      if (opts.include && opts.include.length > 0) {
        _keys = opts.include;
      } else {
        _keys = this._fields.map(v => v.name) as any;
      }
      if (opts.exclude && opts.exclude.length > 0) {
        _keys = _keys.filter(v => !opts.exclude!.includes(v))
      }
      _keys = _keys.map(v => dbctx.quote(this._name, v)) as any;
      keys = _keys.join(", ")
    }

    const tmp = [{ sql: `SELECT ${keys} FROM ${tablename}` }] as Fragment[];
    this._push_where(tmp, where, { allowemptywhere: true });
    this._push_opts(tmp, opts);
    return tmp;
  }
}
