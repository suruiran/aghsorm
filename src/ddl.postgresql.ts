import type { Fragments } from "./frag.js";

export class PostgreSQLDialect {
    static tableopts(opts?: {
        scope?: "GLOBAL" | "LOCAL";
        temp?: boolean;
        unlogged?: boolean;
        ifNotExists?: boolean;
        inherits?: string[];
        onCommit?: "PRESERVE ROWS" | "DELETE ROWS" | "DROP";
        tableSpace?: string;
        using?: string;
        with?: Record<string, any>;
        constraint?: string;
        check?: Fragments;
        withoutOids?: boolean;
        partition?: {
            kind?: "RANGE" | "LIST" | "HASH";
            items: {
                by: string | Fragments;
                collate?: string;
                opclass?: string;
            }[];
        };
    }): Record<string, any> {
        return { ...opts };
    }

    static colopts(opts?: {
        storage?: "PLAIN" | "EXTERNAL" | "EXTENDED" | "MAIN" | "DEFAULT";
        compression?: string;
        collate?: string;
        constraint?: string;
        check?: Fragments;
        as?: {
            frags: Fragments;
            kind?: "virtual" | "stored";
        };
        defferrable?: boolean;
        initDeferred?: boolean;
        enforced?: boolean;
    }): Record<string, any> {
        return { ...opts };
    }

    static indexopts(opts?: {
        unique?: boolean;
        concurrently?: boolean;
        ifNotExists?: boolean;
        only?: boolean;
        using?: string;
        nullsDistinct?: boolean;
        with?: Record<string, any>;
        tablespace?: string;
        where?: Fragments;
    }): Record<string, any> {
        return { ...opts };
    }
}