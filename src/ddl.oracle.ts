import { ColOptsBuilder, OptsBuild } from "./builder.js";
import { IDDLColOpts } from "./ddl.js";
import { Fragments } from "./frag.js";

export class OracleDialect {
    static tableopts(opts?: {
        temporary?: boolean;
        sharded?: boolean;
        duplicated?: boolean;
        immutable?: {
            value: boolean;
            blockchain?: boolean;
        };
        sharing?: "METADATA" | "DATA" | "EXTENDED DATA" | "NONE";

        check?: Fragments;
        period?: {
            valid: string;
            start?: string;
            end?: string;
        };

        collation?: string;
        properties?: {
            physical?: Fragments;
            table?: Fragments;
        };

        memoptimize?: {
            read?: boolean;
            write?: boolean;
        };
        parent?: string;
    }): Record<string, any> {
        return { ...opts };
    }

    static colopts(
        opts: IDDLColOpts,
        extra?: {
            collate?: string;
            sort?: boolean;
            invisible?: boolean;
            encrypt?: Fragments;
            check?: Fragments;
            as?: {
                frag: Fragments;
                virtual?: boolean;
            };
        }
    ): Record<string, any> {
        return { ...opts, extra: { ...opts.extra, ...extra } };
    }

    static indexopts(opts?: {
        type?: "UNIQUE" | "BITMAP" | "MULTIVALUE";
        usable?: boolean;
        deferred?: boolean;
    }): Record<string, any> {
        return { ...opts };
    }

    static readonly builders = {
        tableopts() {
            return new OptsBuild<NonNullable<Parameters<typeof OracleDialect.tableopts>[0]>>();
        },
        colopts() {
            return new ColOptsBuilder<NonNullable<Parameters<typeof OracleDialect.colopts>[1]>>();
        },
        indexopts() {
            return new OptsBuild<NonNullable<Parameters<typeof OracleDialect.indexopts>[0]>>();
        }
    };

    static readonly types = {
        char: chartype("CHAR"),
        varchar2: chartype("VARCHAR2"),
        nchar: sizetype("NCHAR"),
        nvarchar2: chartype("NVARCHAR2"),
        number: (opts?: { precision?: number; scale?: number }) => {
            if (!opts || !opts.precision) return "NUMBER";
            if (opts?.scale == null) return `NUMBER(${opts?.precision})`;
            return `NUMBER(${opts?.precision}, ${opts?.scale})`;
        },
        float: (opts?: { precision?: number; }) => {
            if (!opts || !opts.precision) return "FLOAT";
            return `FLOAT(${opts?.precision})`;
        },
        binaryfloat: () => "BINARY_FLOAT",
        binarydouble: () => "BINARY_DOUBLE",
        long: () => "LONG",
        raw: sizetype("RAW"),
        date: () => "DATE",
        timestamp: (opts?: {
            precision?: number;
            withtz?: boolean;
            local?: boolean;
        }) => {
            let val = "TIMESTAMP";
            if (opts?.precision) {
                val = `${val}(${opts?.precision})`;
            }
            if (opts?.withtz) {
                val = `${val} WITH ${opts?.local ? "LOCAL " : ""}TIME ZONE`;
            }
            return val;
        },
        blob: () => "BLOB",
        clob: () => "CLOB",
        nclob: () => "NCLOB",
        rowid: () => "ROWID",
        urowid: sizetype("UROWID"),
        interval: (kind: "YEAR" | "DAY", precision?: number, opts?: { secondsPrecision?: number }) => {
            let val = `INTERVAL ${kind}`;
            if (precision) {
                val = `${val}(${precision})`;
            }
            if (kind === "YEAR") {
                val = `${val} TO MONTH`;
            } else {
                val = `${val} TO SECOND`;
                if (opts?.secondsPrecision) {
                    val = `${val}(${opts?.secondsPrecision})`;
                }
            }
            return val;
        }
    };
}

function sizetype(base: string) {
    return (opts?: { length?: number }) => {
        if (!opts || !opts.length) return base;
        return `${base}(${opts.length})`
    }
}

function chartype(base: string) {
    return (opts?: { length?: number; type?: "BYTE" | "CHAR" }) => {
        let val = base;
        if (opts?.length) {
            val = `${base}(${opts.length}${opts.type ? ` ${opts.type}` : ""})`
        }
        return val;
    };
}