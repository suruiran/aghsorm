import { ColOptsBuilder, OptsBuild } from "./builder.js";
import { IDDLColOpts } from "./ddl.js";
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

        check?: {
            frags: Fragments;
            inherit?: boolean;
        };
        withoutOids?: boolean;
    }): Record<string, any> {
        return { ...opts };
    }

    static colopts(
        opts: IDDLColOpts,
        extra?: {
            storage?: "PLAIN" | "EXTERNAL" | "EXTENDED" | "MAIN" | "DEFAULT";
            compression?: string;
            collate?: string;
            check?: {
                frags: Fragments;
                inherit?: boolean;
            };
            notNullInherit?: boolean;
            as?: {
                frags: Fragments;
                kind?: "virtual" | "stored";
            };
        }
    ): IDDLColOpts {
        return { ...opts, extra: { ...opts.extra, ...extra } };
    }

    static indexopts(opts?: {
        unique?: boolean;
        concurrently?: boolean;
        ifNotExists?: boolean;
        using?: string;
        nullsDistinct?: boolean;
        with?: Record<string, any>;
        tablespace?: string;
        where?: Fragments;
    }): Record<string, any> {
        return { ...opts };
    }

    static readonly builders = {
        tableopts() {
            return new OptsBuild<NonNullable<Parameters<typeof PostgreSQLDialect.tableopts>[0]>>();
        },
        colopts() {
            return new ColOptsBuilder<NonNullable<Parameters<typeof PostgreSQLDialect.colopts>[1]>>();
        },
        indexopts() {
            return new OptsBuild<NonNullable<Parameters<typeof PostgreSQLDialect.indexopts>[0]>>();
        }
    };

    static readonly types = {
        smallint: () => "smallint",
        integer: () => "integer",
        bigint: () => "bigint",
        decimal: (opts?: { precision?: number; scale?: number }) => {
            if (!opts) return "decimal";
            if (opts.precision == null && opts.scale == null) return "decimal";
            if (opts.scale == null) return `decimal(${opts.precision})`;
            return `decimal(${opts.precision}, ${opts.scale})`;
        },
        real: () => "real",
        double: () => "double precision",
        smallserial: () => "smallserial",
        serial: () => "serial",
        bigserial: () => "bigserial",
        money: () => "money",
        varchar: lentype("varchar"),
        char: lentype("char"),
        text: () => "text",
        bytea: () => "bytea",
        timestamp: timetype("timestamp"),
        date: () => "date",
        time: timetype("time"),
        interval: (opts?: { field?: IntervalFieldKind; precision?: number }) => {
            let val = `interval`;
            if (opts?.field) val = `${val} ${opts.field}`;
            if (opts?.precision) val = `${val}(${opts.precision})`;
            return val;
        },
        boolean: () => "boolean",
        json: () => "json",
        jsonb: () => "jsonb",
        array: (type: string) => `${type}[]`,
    };
}

export type IntervalFieldKind = "YEAR" | "MONTH" | "DAY" | "HOUR" | "MINUTE" | "SECOND"
    | "YEAR TO MONTH" | "DAY TO HOUR" | "DAY TO MINUTE" | "DAY TO SECOND"
    | "HOUR TO MINUTE" | "HOUR TO SECOND" | "MINUTE TO SECOND"

function lentype(type: string) {
    return (opts?: { length?: number }) => {
        if (!opts?.length) return type;
        return `${type}(${opts.length})`;
    }
}

function timetype(type: string) {
    return (opts?: { precision?: number; withtz?: boolean }) => {
        let val = `${type}`;
        if (opts?.precision) val = `${val}(${opts.precision})`;
        if (opts?.withtz) val = `${val} with time zone`;
        return val;
    }
}