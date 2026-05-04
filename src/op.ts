import { Frags, mksqlfrag, type Fragments } from "./frag.js";
import { lazy } from "./lazy.js";
import type { SqlTable } from "./table.js";
import type { Identifier, RawSql, Value } from "./types.js";
import { opItemToSQL } from "./utils.js";
import { getCtx } from "./ctxvals.js";

export type IOpableItems = Value | Identifier | RawSql | Op | SqlTable<any>;
export type ITypedOpableItem<T> = T | Identifier | RawSql | Op;

type OpToSQLFunc = (
    tmp: Fragments,
    left: { val: IOpableItems } | null,
    right: { val: IOpableItems } | null
) => void;

function fmtRightsOp(
    tmp: Fragments,
    op: string,
    left: IOpableItems | undefined,
    items: IOpableItems[]
) {
    if (typeof left !== "undefined") {
        opItemToSQL(left, tmp);
    }
    if (op) {
        tmp.push(mksqlfrag(`${op} (`));
    }

    for (let i = 0; i < items.length; i++) {
        opItemToSQL(items[i]!, tmp);
        if (i < items.length - 1) {
            tmp.push(Frags.comma);
        }
    }

    if (op) {
        tmp.push(Frags.parenthesis.right);
    }
}


function join(
    tmp: Fragments,
    items: IOpableItems[],
    opts: {
        sep: string;
        begin: string;
        end: string;
        allowempty?: boolean;
    }
) {
    if (items.length === 0 && !opts.allowempty) {
        throw new Error("aghsorm.join: items is empty");
    }
    tmp.push(mksqlfrag(opts.begin));
    const sepfrag = mksqlfrag(opts.sep);
    const size = items.length;
    let i = 0;
    for (const ele of items) {
        opItemToSQL(ele, tmp);
        i++;
        if (i < size) {
            tmp.push(sepfrag);
        }
    }
    tmp.push(mksqlfrag(opts.end));
}

function updatelocal(local: any, val: any, ws?: WeakSet<any>) {
    if (!local) return;
    if (typeof local !== "object") return;

    if (ws && ws.has(local)) {
        return;
    }
    ws?.add(local);

    if (local instanceof Op) {
        local.__updatecol(val);
        return;
    }
    if (Array.isArray(local)) {
        for (const ele of local) {
            updatelocal(ele, val, ws);
        }
        return;
    }
    for (const ele of Object.values(local)) {
        updatelocal(ele, val, ws);
    }
}

export class Op {
    /** @internal */
    private _opkind: string;
    /** @internal */
    private _left: { val: IOpableItems } | null;
    /** @internal */
    private _right: { val: IOpableItems } | null;
    /** @internal */
    private _tosql: OpToSQLFunc | null;
    /** @internal */
    private _bracket: boolean;
    /** @internal */
    private _iscol: {
        val: string;
    } | null;
    /** @internal */
    private _local: any;

    constructor(
        opkind: string,
        left: IOpableItems | undefined,
        right: IOpableItems | undefined,
        opts?: {
            fmt?: OpToSQLFunc | null;
            bracket?: boolean;
            /** @internal */
            _iscol?: {
                val: string;
            };
            /** @internal */
            _local?: any;
        }
    ) {
        this._opkind = opkind;
        this._left = typeof left !== "undefined" ? { val: left } : null;
        this._right = typeof right !== "undefined" ? { val: right } : null;
        this._tosql = opts?.fmt || null;
        this._bracket = opts?.bracket || false;
        this._iscol = opts?._iscol || null;
        this._local = opts?._local || null;
    }

    tosql(tmp: Fragments) {
        if (this._iscol != null) {
            if (!this._iscol.val) {
                throw new Error(`aghsorm.ColOp: column name is empty`);
            }
            tmp.push(mksqlfrag(this._iscol.val));
            return;
        }

        if (this._tosql) {
            this._tosql(tmp, this._left, this._right);
            return;
        }
        if (this._left != null) {
            if (this._bracket) tmp.push(Frags.parenthesis.left);
            opItemToSQL(this._left.val, tmp);
            if (this._bracket) tmp.push(Frags.parenthesis.right);
        }
        tmp.push(mksqlfrag(this._opkind));
        if (this._right != null) {
            if (this._bracket) tmp.push(Frags.parenthesis.left);
            opItemToSQL(this._right.val, tmp);
            if (this._bracket) tmp.push(Frags.parenthesis.right);
        }
    }

    /** @internal */
    __updatecol(val: string) {
        if (this._iscol != null) {
            if (this._iscol.val && this._iscol.val !== val) {
                throw new Error(`aghsorm.ColOp: can not be reused, Please use a factory function to do that.`);
            }
            this._iscol.val = val;
        }
        if (this._left != null && this._left.val instanceof Op) {
            this._left.val.__updatecol(val);
        }
        if (this._right != null && this._right.val instanceof Op) {
            this._right.val.__updatecol(val);
        }
        updatelocal(this._local, val, new WeakSet());
    }

    static and(...items: IOpableItems[]) {
        if (items.length === 0) {
            throw new Error("aghsorm.Op.AND: items is empty");
        }
        const local = items;
        return new Op("AND", undefined, undefined, {
            fmt: (tmp) => {
                join(tmp, local, { sep: "AND", begin: "(", end: ")" });
            },
            _local: local,
        });
    }

    and(...items: IOpableItems[]): Op {
        return Op.and(this, ...items);
    }

    static or(...items: IOpableItems[]) {
        if (items.length === 0) {
            throw new Error("aghsorm.Op.OR: items is empty");
        }
        const local = items;
        return new Op("OR", undefined, undefined, {
            fmt: (tmp) => {
                join(tmp, local, { sep: "OR", begin: "(", end: ")" });
            },
            _local: local,
        });
    }

    or(right: IOpableItems): Op {
        return Op.or(this, right);
    }

    not(): Op {
        return new Op("NOT", undefined, this, { bracket: true });
    }

    static eq(left: IOpableItems | undefined, right: IOpableItems | undefined) {
        return new Op("=", left, right);
    }

    eq(right: IOpableItems): Op {
        return Op.eq(this, right);
    }

    static neq(left: IOpableItems | undefined, right: IOpableItems | undefined) {
        return new Op("!=", left, right);
    }

    neq(right: IOpableItems): Op {
        return Op.neq(this, right);
    }

    static gt(left: IOpableItems | undefined, right: IOpableItems | undefined) {
        return new Op(">", left, right);
    }

    gt(right: IOpableItems): Op {
        return Op.gt(this, right);
    }

    static gte(left: IOpableItems | undefined, right: IOpableItems | undefined) {
        return new Op(">=", left, right);
    }

    gte(right: IOpableItems): Op {
        return Op.gte(this, right);
    }

    static lt(left: IOpableItems | undefined, right: IOpableItems | undefined) {
        return new Op("<", left, right);
    }

    lt(right: IOpableItems): Op {
        return Op.lt(this, right);
    }

    static lte(left: IOpableItems | undefined, right: IOpableItems | undefined) {
        return new Op("<=", left, right);
    }

    lte(right: IOpableItems): Op {
        return Op.lte(this, right);
    }

    static bracket(item: IOpableItems): Op {
        const local = item;
        return new Op("", undefined, undefined, {
            fmt: (tmp) => {
                tmp.push(Frags.parenthesis.left)
                opItemToSQL(local, tmp);
                tmp.push(Frags.parenthesis.right);
            },
            _local: local,
        });
    }

    bracket(): Op {
        return Op.bracket(this);
    }

    static in(left: IOpableItems, ...items: IOpableItems[]) {
        if (items.length < 1) {
            throw new Error("aghsorm.Op.IN: items is empty");
        }
        const local = { left, items };
        return new Op("IN", undefined, undefined, {
            fmt: (tmp) => {
                fmtRightsOp(tmp, "IN", local.left, local.items);
            },
            _local: local,
        });
    }

    in(...items: IOpableItems[]): Op {
        return Op.in(this, ...items);
    }

    static notin(left: IOpableItems, ...items: IOpableItems[]) {
        if (items.length < 1) {
            throw new Error("aghsorm.Op.NOTIN: items is empty");
        }
        const local = { left, items };
        return new Op("", undefined, undefined, {
            fmt: (tmp) => {
                fmtRightsOp(tmp, "NOT IN", local.left, local.items);
            },
            _local: local,
        });
    }

    notin(...items: IOpableItems[]): Op {
        return Op.notin(this, ...items);
    }

    static between(left: IOpableItems, begin: IOpableItems, end: IOpableItems) {
        const local = { left, begin, end };
        return new Op("BETWEEN", undefined, undefined, {
            fmt: (tmp) => {
                opItemToSQL(local.left, tmp);
                tmp.push(Frags.between);
                opItemToSQL(local.begin, tmp);
                tmp.push(Frags.and);
                opItemToSQL(local.end, tmp);
            },
            _local: local,
        });
    }

    between(begin: IOpableItems, end: IOpableItems) {
        return Op.between(this, begin, end);
    }

    static like(left: IOpableItems, right: ITypedOpableItem<string>) {
        return new Op("LIKE", left, right);
    }

    like(right: ITypedOpableItem<string>) {
        return Op.like(this, right);
    }

    isnull(): Op {
        return new Op("IS NULL", this, undefined);
    }

    static plus(
        left: ITypedOpableItem<number | bigint>,
        right: ITypedOpableItem<number | bigint>
    ) {
        return new Op("+", left, right);
    }

    plus(right: ITypedOpableItem<number | bigint>): Op {
        return Op.plus(this, right);
    }

    static minus(
        left: ITypedOpableItem<number | bigint>,
        right: ITypedOpableItem<number | bigint>
    ) {
        return new Op("-", left, right);
    }

    minus(right: ITypedOpableItem<number | bigint>): Op {
        return Op.minus(this, right);
    }

    static multiply(
        left: ITypedOpableItem<number | bigint>,
        right: ITypedOpableItem<number | bigint>
    ) {
        return new Op("*", left, right);
    }

    multiply(right: ITypedOpableItem<number | bigint>): Op {
        return Op.multiply(this, right);
    }

    static divide(
        left: ITypedOpableItem<number | bigint>,
        right: ITypedOpableItem<number | bigint>
    ) {
        return new Op("/", left, right);
    }

    divide(right: ITypedOpableItem<number | bigint>): Op {
        return Op.divide(this, right);
    }

    static mod(
        left: ITypedOpableItem<number | bigint>,
        right: ITypedOpableItem<number | bigint>
    ) {
        return new Op("%", left, right);
    }

    mod(right: ITypedOpableItem<number | bigint>): Op {
        return Op.mod(this, right);
    }

    static pow(
        left: ITypedOpableItem<number | bigint>,
        right: ITypedOpableItem<number | bigint>
    ) {
        return new Op("^", left, right);
    }

    pow(right: ITypedOpableItem<number | bigint>): Op {
        return Op.pow(this, right);
    }

    static lshift(
        left: ITypedOpableItem<number | bigint>,
        right: ITypedOpableItem<number | bigint>
    ) {
        return new Op("<<", left, right);
    }

    lshift(right: ITypedOpableItem<number | bigint>): Op {
        return Op.lshift(this, right);
    }

    static rshift(
        left: ITypedOpableItem<number | bigint>,
        right: ITypedOpableItem<number | bigint>
    ) {
        return new Op(">>", left, right);
    }

    rshift(right: ITypedOpableItem<number | bigint>): Op {
        return Op.rshift(this, right);
    }

    static call(funcname: string, ...args: IOpableItems[]) {
        const local = args;
        return new Op("CALL", undefined, undefined, {
            fmt: (tmp) => {
                tmp.push(mksqlfrag(funcname));
                tmp.push(Frags.parenthesis.left);
                switch (local.length) {
                    case 0: {
                        break;
                    }
                    default: {
                        fmtRightsOp(tmp, "", undefined, local);
                        break;
                    }
                }
                tmp.push(Frags.parenthesis.right);
                return tmp;
            },
            _local: local,
        });
    }

    alias(name: string): Op {
        return new Op("AS", this, undefined, {
            fmt: (tmp) => {
                tmp.push(Frags.parenthesis.left);
                opItemToSQL(this, tmp);
                tmp.push(Frags.parenthesis.right);
                tmp.push(mksqlfrag("AS"));
                const ctx = getCtx();
                tmp.push(mksqlfrag(ctx ? ctx.quote("id", name) : name));
                return tmp;
            },
        });
    }
}

lazy.Op = Op;

export const ColOp = new Proxy<Op>({} as any, {
    get(_target, prop) {
        if (typeof prop !== "string" || !(prop in Op.prototype)) return undefined;
        const colop = new Op("COL PLACEHOLDER", undefined, undefined, { _iscol: { val: "" } });
        const fnc = Reflect.get(colop, prop, colop) as () => void;
        return fnc.bind(colop);
    },
});
