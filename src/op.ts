import { type Fragment, Frags, mksqlfrag } from "./frag.js";
import { lazy } from "./lazy.js";
import type { Identifier, RawSql, Value } from "./types.js";
import { opItemToSQL } from "./utils.js";

export type IOpableItems = Value | Identifier | RawSql | Op;
export type ITypedOpableItem<T> = T | Identifier | RawSql | Op;

type OpToSQLFunc = (
    tmp: Fragment[],
    left: { val: IOpableItems } | null,
    right: { val: IOpableItems } | null
) => void;

function fmtRightsOp(
    tmp: Fragment[],
    op: string,
    left: IOpableItems | undefined,
    item: IOpableItems,
    ...items: IOpableItems[]
) {
    if (typeof left !== "undefined") {
        opItemToSQL(left, tmp);
    }
    if (op) {
        tmp.push(mksqlfrag(`${op} (`));
    }

    const eles = [item, ...items];
    for (let i = 0; i < eles.length; i++) {
        opItemToSQL(eles[i]!, tmp);
        if (i < eles.length - 1) {
            tmp.push(Frags.comma);
        }
    }

    if (op) {
        tmp.push(Frags.parenthesis.right);
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

    constructor(
        opkind: string,
        left: IOpableItems | undefined,
        right: IOpableItems | undefined,
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

    tosql(tmp: Fragment[]) {
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

    static and(left: IOpableItems | undefined, right: IOpableItems | undefined) {
        return new Op("AND", left, right, { bracket: true });
    }

    and(right: IOpableItems): Op {
        return Op.and(this, right);
    }

    static or(left: IOpableItems | undefined, right: IOpableItems | undefined) {
        return new Op("OR", left, right, { bracket: true });
    }

    or(right: IOpableItems): Op {
        return Op.or(this, right);
    }

    not(): Op {
        return new Op("NOT", null, this, { bracket: true });
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
        return new Op("", item, null, {
            fmt: (tmp) => {
                tmp.push(Frags.parenthesis.left)
                opItemToSQL(item, tmp);
                tmp.push(Frags.parenthesis.right);
            },
        });
    }

    bracket(): Op {
        return Op.bracket(this);
    }

    static in(left: IOpableItems, item: IOpableItems, ...items: IOpableItems[]) {
        return new Op("", left, null, {
            fmt: (tmp) => {
                fmtRightsOp(tmp, "IN", left, item, ...items)
            },
        });
    }

    in(item: IOpableItems, ...items: IOpableItems[]) {
        return Op.in(this, item, ...items);
    }

    static notin(
        left: IOpableItems,
        item: IOpableItems,
        ...items: IOpableItems[]
    ) {
        return new Op("", left, null, {
            fmt: (tmp) => {
                fmtRightsOp(tmp, "NOT IN", left, item, ...items);
            },
        });
    }

    notin(item: IOpableItems, ...items: IOpableItems[]) {
        return Op.notin(this, item, ...items);
    }

    static between(left: IOpableItems, begin: IOpableItems, end: IOpableItems) {
        return new Op("", left, null, {
            fmt: (tmp) => {
                opItemToSQL(left, tmp);
                tmp.push(Frags.between);
                opItemToSQL(begin, tmp);
                tmp.push(Frags.and);
                opItemToSQL(end, tmp);
            },
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
        return new Op("IS NULL", this, null);
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
        return new Op("", null, null, {
            fmt: (tmp) => {
                tmp.push(mksqlfrag(funcname));
                tmp.push(Frags.parenthesis.left);
                switch (args.length) {
                    case 0: {
                        break;
                    }
                    default: {
                        const [fa, ...rest] = args;
                        fmtRightsOp(tmp, "", undefined, fa!, ...rest);
                    }
                }
                tmp.push(Frags.parenthesis.right);
                return tmp;
            },
        });
    }
}

lazy.Op = Op;