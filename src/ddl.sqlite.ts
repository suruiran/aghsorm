import { ColOptsBuilder, OptsBuild } from "./builder.js";
import { type IDDLColOpts } from "./ddl.js";
import { type Fragments } from "./frag.js";

export type SqliteConflict = "rollback" | "abort" | "fail" | "ignore" | "replace";

export class SqliteDialect {
    static tableopts(
        opt?: {
            temp?: boolean;
            ifNotExists?: boolean;
            withoutRowid?: boolean;
            strict?: boolean;

            unique?: {
                cols: string[];
                conflict?: SqliteConflict;
            }
            check?: Fragments;
            primaryConflict?: SqliteConflict;
        }
    ): Record<string, any> {
        return { ...opt };
    }

    static colopts(
        opts: IDDLColOpts,
        ext?: {
            conflicts?: {
                notnull?: SqliteConflict;
                unique?: SqliteConflict;
            },
            check?: Fragments;
            collate?: string;
            as?: {
                frags?: Fragments;
                kind?: "virtual" | "stored";
            };
        }
    ): IDDLColOpts {
        return { ...opts, extra: { ...opts.extra, ...ext } };
    }

    static indexopts(opt?: { unique?: boolean; ifNotExists?: boolean }): Record<string, any> {
        return { ...opt };
    }

    static readonly types = {
        text: () => "TEXT",
        blob: () => "BLOB",
        integer: () => "INTEGER",
        real: () => "REAL",
    };

    static readonly builders = {
        tableopts() {
            return new OptsBuild<NonNullable<Parameters<typeof SqliteDialect.tableopts>[0]>>();
        },
        colopts() {
            return new ColOptsBuilder<NonNullable<Parameters<typeof SqliteDialect.colopts>[1]>>();
        },
        indexopts() {
            return new OptsBuild<NonNullable<Parameters<typeof SqliteDialect.indexopts>[0]>>();
        }
    }
}