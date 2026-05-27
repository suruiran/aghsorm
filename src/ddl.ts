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
    sqltype: string | Fragments;
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
