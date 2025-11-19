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

let ctx: DBContext | null = null;

export function setdbcontext(dbctx: DBContext) {
  ctx = dbctx;
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
      fmt: () => {
        let txt = this._name;
        if (ctx) {
          txt = ctx.quote(this._table, this._name);
        }
        return [{ sql: txt }];
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
      fmt: () => [{ sql: this._sql }],
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

export type OpItem = Value | Identifier | RawSql | Op;
export type SimpleOpItem<T> = T | Identifier | RawSql | Op;

// #region utils

function opItemToSQL(item: OpItem, temp: Fragment[]) {
  if (item instanceof Identifier) {
    temp.push(...item.op().tosql());
    return;
  }
  if (item instanceof RawSql) {
    temp.push({ sql: item._sql });
    return;
  }
  if (item instanceof Op) {
    temp.push(...item.tosql());
    return;
  }
  temp.push({ value: item });
}

function fmtRightsOp(
  op: string,
  left: OpItem | undefined,
  item: OpItem,
  ...items: OpItem[]
) {
  const tmp = [] as Fragment[];
  if (typeof left !== "undefined") {
    opItemToSQL(left, tmp);
  }
  if (op) {
    tmp.push({ sql: `${op} (` });
  }

  const eles = [item, ...items];
  for (let i = 0; i < eles.length; i++) {
    opItemToSQL(eles[i]!, tmp);
    if (i < eles.length - 1) {
      tmp.push({ sql: ", " });
    }
  }

  if (op) {
    tmp.push({ sql: ")" });
  }
  return tmp;
}

function pushOrder<T>(temp: Fragment[], orderby?: IOrder<T>[]) {
  if (!orderby) return;
  for (const item of orderby) {
    if (typeof item === "string") {
      temp.push({ sql: ctx?.quote(null, item) || item });
    } else {
      temp.push({
        sql: `${ctx?.quote(null, item.field) || item.field} ${item.direction}`,
      });
    }
  }
}

function pushLimit(
  temp: Fragment[],
  opts?: { limit?: number; offset?: number }
) {
  if (!opts) return;
  if (opts.limit) {
    temp.push({ sql: `LIMIT` });
    temp.push({ value: opts.limit });
  }
  if (opts.offset) {
    temp.push({ sql: `OFFSET` });
    temp.push({ value: opts.offset });
  }
}

// #endregion

export interface Fragment {
  sql?: string;
  value?: Value;
}

type OpToSQLFunc = (
  left: { val: OpItem } | null,
  right: { val: OpItem } | null
) => Fragment[];

export class Op {
  private _opkind: string;
  private _left: { val: OpItem } | null;
  private _right: { val: OpItem } | null;
  private _tosql: OpToSQLFunc | null;
  private _bracket: boolean;

  constructor(
    opkind: string,
    left: OpItem | undefined,
    right: OpItem | undefined,
    opts?: {
      fmt?: OpToSQLFunc | null;
      bracket?: boolean;
    }
  ) {
    this._opkind = opkind;
    this._left = typeof left !== "undefined" ? { val: left } : null;
    this._right = typeof right !== "undefined" ? { val: right } : null;
    this._tosql = opts?.fmt || null;
    this._bracket = opts?.bracket || false;
  }

  tosql(): Fragment[] {
    if (this._tosql) {
      return this._tosql(this._left, this._right);
    }

    const tmp = [] as Fragment[];

    if (this._left != null) {
      if (this._bracket) tmp.push({ sql: "(" });
      opItemToSQL(this._left.val, tmp);
      if (this._bracket) tmp.push({ sql: ")" });
    }
    tmp.push({ sql: this._opkind });
    if (this._right != null) {
      if (this._bracket) tmp.push({ sql: "(" });
      opItemToSQL(this._right.val, tmp);
      if (this._bracket) tmp.push({ sql: ")" });
    }

    return tmp;
  }

  // #region ops
  static and(left: OpItem | undefined, right: OpItem | undefined) {
    return new Op("AND", left, right, { bracket: true });
  }

  and(right: OpItem): Op {
    return Op.and(this, right);
  }

  static or(left: OpItem | undefined, right: OpItem | undefined) {
    return new Op("OR", left, right, { bracket: true });
  }

  or(right: OpItem): Op {
    return Op.or(this, right);
  }

  not(): Op {
    return new Op("NOT", null, this, { bracket: true });
  }

  static eq(left: OpItem | undefined, right: OpItem | undefined) {
    return new Op("=", left, right);
  }

  eq(right: OpItem): Op {
    return Op.eq(this, right);
  }

  static neq(left: OpItem | undefined, right: OpItem | undefined) {
    return new Op("!=", left, right);
  }

  neq(right: OpItem): Op {
    return Op.neq(this, right);
  }

  static gt(left: OpItem | undefined, right: OpItem | undefined) {
    return new Op(">", left, right);
  }

  gt(right: OpItem): Op {
    return Op.gt(this, right);
  }

  static gte(left: OpItem | undefined, right: OpItem | undefined) {
    return new Op(">=", left, right);
  }

  gte(right: OpItem): Op {
    return Op.gte(this, right);
  }

  static lt(left: OpItem | undefined, right: OpItem | undefined) {
    return new Op("<", left, right);
  }

  lt(right: OpItem): Op {
    return Op.lt(this, right);
  }

  static lte(left: OpItem | undefined, right: OpItem | undefined) {
    return new Op("<=", left, right);
  }

  lte(right: OpItem): Op {
    return Op.lte(this, right);
  }

  static bracket(item: OpItem): Op {
    return new Op("", item, null, {
      fmt: () => {
        const tmp = [{ sql: "(" }] as Fragment[];
        opItemToSQL(item, tmp);
        tmp.push({ sql: ")" });
        return tmp;
      },
    });
  }

  bracket(): Op {
    return Op.bracket(this);
  }

  static in(left: OpItem, item: OpItem, ...items: OpItem[]) {
    return new Op("", left, null, {
      fmt: () => {
        return fmtRightsOp("IN", left, item, ...items);
      },
    });
  }

  in(item: OpItem, ...items: OpItem[]) {
    return Op.in(this, item, ...items);
  }

  static notin(left: OpItem, item: OpItem, ...items: OpItem[]) {
    return new Op("", left, null, {
      fmt: () => {
        return fmtRightsOp("NOT IN", left, item, ...items);
      },
    });
  }

  notin(item: OpItem, ...items: OpItem[]) {
    return Op.notin(this, item, ...items);
  }

  static between(left: OpItem, begin: OpItem, end: OpItem) {
    return new Op("", left, null, {
      fmt: () => {
        const tmp = [] as Fragment[];
        opItemToSQL(left, tmp);
        tmp.push({ sql: "BETWEEN" });
        opItemToSQL(begin, tmp);
        tmp.push({ sql: "AND" });
        opItemToSQL(end, tmp);
        return tmp;
      },
    });
  }

  between(begin: OpItem, end: OpItem) {
    return Op.between(this, begin, end);
  }

  static like(left: OpItem, right: SimpleOpItem<string>) {
    return new Op("LIKE", left, right);
  }

  like(right: SimpleOpItem<string>) {
    return Op.like(this, right);
  }

  isnull(): Op {
    return new Op("IS NULL", this, null);
  }

  static plus(
    left: SimpleOpItem<number | bigint>,
    right: SimpleOpItem<number | bigint>
  ) {
    return new Op("+", left, right);
  }

  plus(right: SimpleOpItem<number | bigint>): Op {
    return Op.plus(this, right);
  }

  static minus(
    left: SimpleOpItem<number | bigint>,
    right: SimpleOpItem<number | bigint>
  ) {
    return new Op("-", left, right);
  }

  minus(right: SimpleOpItem<number | bigint>): Op {
    return Op.minus(this, right);
  }

  static multiply(
    left: SimpleOpItem<number | bigint>,
    right: SimpleOpItem<number | bigint>
  ) {
    return new Op("*", left, right);
  }

  multiply(right: SimpleOpItem<number | bigint>): Op {
    return Op.multiply(this, right);
  }

  static divide(
    left: SimpleOpItem<number | bigint>,
    right: SimpleOpItem<number | bigint>
  ) {
    return new Op("/", left, right);
  }

  divide(right: SimpleOpItem<number | bigint>): Op {
    return Op.divide(this, right);
  }

  static mod(
    left: SimpleOpItem<number | bigint>,
    right: SimpleOpItem<number | bigint>
  ) {
    return new Op("%", left, right);
  }

  mod(right: SimpleOpItem<number | bigint>): Op {
    return Op.mod(this, right);
  }

  static pow(
    left: SimpleOpItem<number | bigint>,
    right: SimpleOpItem<number | bigint>
  ) {
    return new Op("^", left, right);
  }

  pow(right: SimpleOpItem<number | bigint>): Op {
    return Op.pow(this, right);
  }

  static lshift(
    left: SimpleOpItem<number | bigint>,
    right: SimpleOpItem<number | bigint>
  ) {
    return new Op("<<", left, right);
  }

  lshift(right: SimpleOpItem<number | bigint>): Op {
    return Op.lshift(this, right);
  }

  static rshift(
    left: SimpleOpItem<number | bigint>,
    right: SimpleOpItem<number | bigint>
  ) {
    return new Op(">>", left, right);
  }

  rshift(right: SimpleOpItem<number | bigint>): Op {
    return Op.rshift(this, right);
  }

  static call(funcname: string, ...args: OpItem[]) {
    if (ctx && ctx.checkfuncname && !ctx.checkfuncname(funcname)) {
      throw new Error(`Invalid function name: ${funcname}`);
    }

    return new Op("", null, null, {
      fmt: () => {
        const tmp = [] as Fragment[];
        tmp.push({ sql: funcname });
        tmp.push({ sql: "(" });
        switch (args.length) {
          case 0: {
            break;
          }
          default: {
            const [fa, ...rest] = args;
            tmp.push(...fmtRightsOp("", undefined, fa!, ...rest));
          }
        }
        tmp.push({ sql: ")" });
        return tmp;
      },
    });
  }

  // #endregion
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
    ? SimpleOpItem<T[K]> | null | undefined
    : SimpleOpItem<T[K]> | null;
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

export class Table<
  T extends { [K in keyof T]: Value },
  PKS extends readonly (keyof T)[]
> {
  private _name: string;
  private _fields: IColumn[];
  private _indexes: IIndex[];
  constructor(name: string, fields: IColumn[], indexes: IIndex[]) {
    this._name = name;
    this._fields = fields;
    this._indexes = indexes;
  }

  id(key: keyof T & string): Op {
    return new Identifier(key, this._name).op();
  }

  newcol(newcol: IColumn) {}
  dropcol(col: string) {}
  modcol(fromcol: string, tocol: IColumn): void {}

  insert(record: InsertRecord<T, PKS>): Fragment[] {
    let tablename = this._name;
    if (ctx) {
      tablename = ctx.quote(this._name, null);
    }

    const pairs = Array.from(Object.entries(record)).filter(
      ([, v]) => typeof v !== "undefined"
    );
    if (pairs.length === 0) {
      throw new Error("Record must have at least one field for insert");
    }
    const tmp = [] as Fragment[];
    tmp.push({ sql: `INSERT INTO ${tablename}` });
    tmp.push({ sql: "(" });
    tmp.push(
      ...pairs.map(([key]) => ({ sql: ctx ? ctx.quote(null, key) : key }))
    );
    tmp.push({ sql: ")" });
    tmp.push({ sql: "VALUES" });
    tmp.push({ sql: "(" });
    for (const [, item] of pairs) {
      opItemToSQL(item as OpItem, tmp);
    }
    tmp.push({ sql: ")" });
    return tmp;
  }

  delete(
    where: PartialRecord<T> | Op,
    opts?: {
      orderby?: IOrder<T>[];
      limit?: number;
      offset?: number;
    }
  ): Fragment[] {
    let tablename = this._name;
    if (ctx) {
      tablename = ctx.quote(this._name, null);
    }
    const tmp = [{ sql: `DELETE FROM ${tablename}` }] as Fragment[];
    let whereop: Op | null = null;
    if (!(where instanceof Op)) {
      let op: Op | null = null;
      for (const [key, value] of Object.entries(where)) {
        const _op = this.id(key as any).eq(value as OpItem | Value);
        if (op) {
          op = op.and(_op);
        } else {
          op = _op;
        }
      }
      whereop = op;
    } else {
      whereop = where;
    }
    if (!whereop) {
      throw new Error("Where clause is required for delete");
    }
    tmp.push({ sql: "WHERE" });
    tmp.push(...whereop.tosql());

    if (opts) {
      pushOrder(tmp, opts.orderby);
      pushLimit(tmp, opts);
    }
    return tmp;
  }

  update(
    data: PartialRecord<T>,
    where: PartialRecord<T> | Op,
    opts?: {
      orderby?: IOrder<T>[];
      limit?: number;
      offset?: number;
    }
  ): Fragment[] {
    return [];
  }

  select(
    where: PartialRecord<T> | Op,
    opts?: {
      include?: (keyof T)[];
      exclude?: (keyof T)[];
      groupby?: (keyof T)[];
      orderby?: IOrder<T>[];
      limit?: number;
      offset?: number;
    }
  ): Fragment[] {
    return [];
  }
}

interface User {
  id: number;
  name: string;
  age: number;
  birthday: Date;
  avatar: Uint8Array;
  is_admin: boolean;
  created_at: Date;
}

const user = new Table<User, ["id"]>("user", [], []);

console.log(
  user.delete(user.id("id").gt(Op.plus(1, 2).bracket()).bracket().bracket(), {
    orderby: ["age"],
    limit: 10,
  })
);
