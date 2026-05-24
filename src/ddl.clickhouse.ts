import { ColOptsBuilder, OptsBuild } from "./builder.js";
import { IDDLColOpts } from "./ddl.js";
import { Fragments } from "./frag.js";
import { QuoteSQLStringLiteral } from "./utils.js";

export class ClickhouseDialect {
    static readonly ClassName = "ClickhouseDialect";

    static tableopts(opts?: {
        temp?: boolean;
        ifNotExists?: boolean;
        engine?: string;
        comment?: string;
        cluster?: string;
        orderBy?: Fragments;
        check?: {
            name: string;
            frags: Fragments;
        };
        assume?: {
            name: string;
            frags: Fragments;
        }
    }): Record<string, any> {
        return { ...opts };
    }

    static colopts(opts: IDDLColOpts, extra?: {
        materialized?: Fragments;
        ephemeral?: Fragments;
        alias?: Fragments;
        ttl?: Fragments;
        compress?: string;
    }): Record<string, any> {
        return { ...opts, extra: { ...opts.extra, ...extra } };
    }

    static indexopts(opts?: {
        ifNotExists?: boolean;
        type?: string;
        granularity?: number;
    }): Record<string, any> {
        return { ...opts };
    }

    static readonly builders = {
        tableopts() {
            return new OptsBuild<NonNullable<Parameters<typeof ClickhouseDialect.tableopts>[0]>>();
        },
        colopts() {
            return new ColOptsBuilder<NonNullable<Parameters<typeof ClickhouseDialect.colopts>[1]>>();
        },
        indexopts() {
            return new OptsBuild<NonNullable<Parameters<typeof ClickhouseDialect.indexopts>[0]>>();
        }
    };

    static readonly types = {
        int8: inttype("Int8"),
        int16: inttype("Int16"),
        int32: inttype("Int32"),
        int64: inttype("Int64"),
        int128: inttype("Int128"),
        int256: inttype("Int256"),
        float32: () => "Float32",
        float64: () => "Float64",
        bfloat16: () => "BFloat16",
        decimal: (opts?: { precision?: number; scale?: number }) => {
            if (!opts || !opts.precision) return "DECIMAL";
            if (opts.scale == null) return `DECIMAL(${opts.precision})`;
            return `DECIMAL(${opts.precision},${opts.scale})`;
        },
        decimal32: scaletype("DECIMAL32"),
        decimal64: scaletype("DECIMAL64"),
        decimal128: scaletype("DECIMAL128"),
        decimal256: scaletype("DECIMAL256"),
        string: () => "String",
        fixedstring: sizetype("FixedString"),
        date: () => "Date",
        date32: () => "Date32",
        time: () => "Time",
        datetime: (opts?: { tz?: string }) => {
            if (!opts?.tz) return "DateTime";
            return `DateTime(${QuoteSQLStringLiteral(opts.tz)})`;
        },
        time64: precisiontype("Time64"),
        datetime64: (opts?: { tz?: string; precision?: number }) => {
            let val = "DateTime64";
            const precision = opts?.precision ?? 3;
            if (opts?.tz) {
                return `${val}(${precision}, ${QuoteSQLStringLiteral(opts.tz)})`;
            }
            return `${val}(${precision})`;
        },
        enum: (opts: { items: Iterable<string> | Iterable<[string, number]> }) => {
            const items = [] as [string, number][];
            let idx = 0;
            for (const item of opts.items) {
                idx++;
                if (Array.isArray(item)) {
                    items.push(item);
                    continue;
                }
                items.push([item, idx]);
            }
            return `Enum(${items.map(([k, n]) => `${QuoteSQLStringLiteral(k)} = ${n}`).join(", ")})`;
        },
        uuid: () => "UUID",
        ipv4: () => "IPv4",
        ipv6: () => "IPv6",
        boolean: () => "Boolean",
        json: () => "JSON",

        array: (type: string) => `Array(${type})`,
        map: (keytype: string, valtype: string) => `Map(${keytype}, ${valtype})`,
        nullable: (type: string) => `Nullable(${type})`,
    };
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

function scaletype(base: string) {
    return (opts?: { scale?: number }) => {
        if (!opts || !opts.scale) return base;
        return `${base}(${opts.scale})`;
    }
}

function precisiontype(base: string) {
    return (opts?: { precision?: number }) => {
        if (!opts || !opts.precision) return base;
        return `${base}(${opts.precision})`;
    }
}