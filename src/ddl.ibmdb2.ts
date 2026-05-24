import { ColOptsBuilder, OptsBuild } from "./builder.js";
import { IDDLColOpts } from "./ddl.js";
import { Fragments } from "./frag.js";

export class Ibmdb2Dialect {
    static readonly ClassName = "Ibmdb2Dialect";

    static tableopts(opts?: {
        in?: {
            database?: string;
            tablespace: string;
        } | { database: string } | { accelerator: string };
        editproc?: {
            name: string;
            withRowAttrs?: boolean;
        };
        validproc?: string;
        audit?: "NONE" | "CHANGES" | "ALL";
        obid?: number;
        dataCapture?: "NONE" | "CHANGES";
        withRestrictOnDrop?: boolean;
        volatile?: {
            enable: boolean;
            cardinality?: boolean;
        };
        logged?: boolean;
        compress?: "NO" | "YES" | "YES FIXEDLENGTH" | "YES HUFFMAN";
        append?: boolean;
        dssizeInGB?: number;
        pageenum?: "RELATIVE" | "ABSOLUTE";
        keyLabel?: string;

        period?: {
            kind: "SYSTEM_TIME" | "BUSINESS_TIME";
            start: string;
            end: string;
            inclusive?: boolean;
        };
        check?: Fragments;
    }): Record<string, any> {
        return { ...opts };
    }

    static colopts(
        opts: IDDLColOpts,
        extra?: {
            as?: {
                expr: Fragments;
            } | {
                start?: number | bigint;
                incr?: number | bigint;
                min?: number | bigint;
                max?: number | bigint;
                cycle?: boolean;
                cache?: number;
                order?: boolean;
            };
            fieldproc?: string;
            asSecurityLabel?: boolean;
            implicitlyHidden?: boolean;
            inlineLength?: number;
            check?: Fragments;
        },
    ): Record<string, any> {
        return { ...opts, extra: { ...opts.extra, ...extra } };
    }

    static indexopts(opts?: {
        unique?: boolean;
        include?: string[];
        cluster?: boolean;
        partitioned?: string;
        padded?: boolean;
        define?: boolean;
        compress?: boolean;
        includeNull?: boolean;
    }): Record<string, any> {
        return { ...opts };
    }

    static readonly builders = {
        tableopts() {
            return new OptsBuild<NonNullable<Parameters<typeof Ibmdb2Dialect.tableopts>[0]>>();
        },
        colopts() {
            return new ColOptsBuilder<NonNullable<Parameters<typeof Ibmdb2Dialect.colopts>[1]>>();
        },
        indexopts() {
            return new OptsBuild<NonNullable<Parameters<typeof Ibmdb2Dialect.indexopts>[0]>>();
        }
    }

    static readonly types = {
        smallint: () => "SMALLINT",
        int: () => "INT",
        bigint: () => "BIGINT",
        decimal: decimaltype("DECIMAL"),
        numeric: decimaltype("NUMERIC"),
        float: floattype("FLOAT"),
        double: floattype("DOUBLE"),
        real: () => "REAL",
        decfloat: (opts?: { n?: 34 | 16 }) => {
            if (!opts || !opts.n) return "DECFLOAT";
            return `DECFLOAT(${opts.n})`;
        },
        char: charstype("CHAR"),
        varchar: charstype("VARCHAR"),
        clob: charstype("CLOB"),
        binary: lentype("BINARY"),
        varbinary: lentype("VARBINARY"),
        blob: lentype("BLOB"),
        date: () => "DATE",
        time: () => "TIME",
        timestamp: (opts?: { precision?: number, withtz?: boolean }) => {
            let val = "TIMESTAMP";
            if (opts?.precision != null) {
                val = `${val}(${opts.precision})`;
            }
            if (opts?.withtz != null) {
                if (opts.withtz) {
                    val = `${val} WITH TIME ZONE`;
                } else {
                    val = `${val} WITHOUT TIME ZONE`;
                }
            }
            return val;
        },
        rowid: () => "ROWID",
    }
}


function decimaltype(base: string) {
    return (opts?: { precision?: number; scale?: number }) => {
        if (!opts || !opts.precision) return base;
        if (opts.scale == null) {
            return `${base}(${opts.precision})`;
        }
        return `${base}(${opts.precision},${opts.scale})`;
    }
}

function floattype(base: string) {
    return (opts?: { n?: number }) => {
        if (!opts || !opts.n) return base;
        return `${base}(${opts.n})`;
    }
}

function lentype(base: string) {
    return (opts?: { length?: number }) => {
        if (!opts || !opts.length) return base;
        return `${base}(${opts.length})`;
    }
}

function charstype(base: string) {
    return (opts?: { length?: number; encoding?: "SBCS" | "MIXED" | "BIT" | "UTF8" }) => {
        let val = lentype(base)({ length: opts?.length as any });
        if (opts?.encoding != null) {
            switch (opts.encoding) {
                case "UTF8": {
                    val = `${val} CCSID 1208`;
                    break;
                }
                default: {
                    val = `${val} FOR ${opts.encoding} DATA`;
                    break;
                }
            }
        }
        return val;
    }
}
