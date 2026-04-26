import { ColOptsBuilder, OptsBuild } from "./builder.js";
import { IDDLColOpts } from "./ddl.js";
import { Fragments } from "./frag.js";

export class DuckdbDialect {
    static tableopts(opts?: {
        temp?: boolean;
        ifNotExists?: boolean;
        withNoData?: boolean;
        check?: Fragments;
    }): Record<string, any> {
        return { ...opts };
    }

    static colopts(opts: IDDLColOpts, extra?: {
        as?: {
            frag: Fragments;
            virtual?: boolean;
        }
        check?: Fragments;
        collate?: string;
    }): Record<string, any> {
        return { ...opts, extra: { ...opts.extra, ...extra } };
    }

    static indexopts(opts?: {
        unique?: boolean;
        ifNotExists?: boolean;
        using?: string;
        with?: Record<string, any>;
    }): Record<string, any> {
        return { ...opts };
    }

    static readonly builders = {
        tableopts() {
            return new OptsBuild<NonNullable<Parameters<typeof DuckdbDialect.tableopts>[0]>>();
        },
        colopts() {
            return new ColOptsBuilder<NonNullable<Parameters<typeof DuckdbDialect.colopts>[1]>>();
        },
        indexopts() {
            return new OptsBuild<NonNullable<Parameters<typeof DuckdbDialect.indexopts>[0]>>();
        }
    }

    static readonly types = {
        bigint: inttype("BIGINT"),
        bit: () => "BIT",
        blob: sizetype("BLOB"),
        bignum: sizetype("BIGNUM"),
        boolean: () => "BOOLEAN",
        date: () => "DATE",
        decimal: (opts?: { precision?: number; scale?: number }) => {
            if (!opts || !opts.precision) return "DECIMAL";
            if (opts.scale == null) return `DECIMAL(${opts.precision})`;
            return `DECIMAL(${opts.precision},${opts.scale})`;
        },
        double: () => "DOUBLE",
        float: () => "FLOAT",
        hugeint: inttype("HUGEINT"),
        integer: inttype("INTEGER"),
        interval: () => "INTERVAL",
        smallint: inttype("SMALLINT"),
        time: () => "TIME",
        timestamp: (opts?: { withtz?: boolean }) => {
            if (opts?.withtz) return "TIMESTAMP WITH TIME ZONE";
            return "TIMESTAMP";
        },
        tinyint: inttype("TINYINT"),
        uuid: () => "UUID",
        varchar: sizetype("VARCHAR"),
        json: () => "JSON",

        array: (ele: string, len: number) => {
            return `${ele}[${len}]`;
        },
        list: (ele: string) => {
            return `${ele}[]`;
        },
        map: (key: string, val: string) => {
            return `map(${key}, ${val})`;
        }
    }
}

function inttype(base: string) {
    return (opts?: { unsigned?: boolean }) => {
        if (!opts?.unsigned) return base;
        return `U${base}`;
    }
}

function sizetype(base: string) {
    return (opts?: { length?: number }) => {
        if (!opts || !opts.length) return base;
        return `${base}(${opts.length})`;
    }
}