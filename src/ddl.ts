import { type Fragments } from "./frag.js";
import { type Value } from "./types.js";

export interface IDDLColOpts {
    primary?: boolean;
    autoincr?: boolean;
    nullable?: boolean;
    unique?: boolean;
    comment?: string;
    default?: Value | Fragments;
    extra?: Record<string, any>;
}

export interface IDDLCol {
    name: string;
    sqltype: string;
    opts?: IDDLColOpts;
}

export interface ITableDDL<K> {
    addcol(col: IDDLCol): Fragments;
    modcoltype(from: K, sqltype: string): Fragments;
    renamecol(from: K, to: string): Fragments;
    dropcol(name: K): Fragments;

    addindex(name: string, cols: K[], opts?: Record<string, any>): Fragments;
    dropindex(name: string): Fragments;
}

export interface IDBDDL {
    addtable(name: string, cols: IDDLCol[], opts?: Record<string, any>): Fragments;
}

export function renderddl(frags: Fragments): string {
    const tmp = [] as string[];
    const len = frags.length;
    let i = 0;
    for (const ele of frags) {
        if (typeof ele.sql === "string") {
            tmp.push(ele.sql);
            i++;
            if (i < len && (ele.sql.trim() !== "")) {
                tmp.push(" ");
            }
            continue;
        }
        throw new Error("Invalid fragment type");
    }
    return tmp.join("");
}