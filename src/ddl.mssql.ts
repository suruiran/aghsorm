import { ColOptsBuilder, OptsBuild } from "./builder.js";
import { IDDLColOpts } from "./ddl.js";
import { Fragments } from "./frag.js";

export class MssqlDialect {
    static tableopts(opts?: {
        check?: Fragments;
        with?: Record<string, any>;
    }): Record<string, any> {
        return { ...opts };
    }

    static colopts(
        opts: IDDLColOpts,
        extra?: {
            filestream?: boolean;
            collate?: string;
            sparse?: boolean;
            maskedWith?: string;
            identity?: {
                seed: number;
                increment: number;
            };
            rowGuidCol?: boolean;
            encrypted?: {
                key?: string;
                type?: "DETERMINISTIC" | "RANDOMIZED";
                algorithm?: string;
            }
            check?: Fragments;
        }
    ): Record<string, any> {
        return { ...opts, extra: { ...opts.extra, ...extra } };
    }

    static indexopts(opts?: {
        unique?: boolean;
        clustered?: boolean;
        where?: Fragments;
        with?: Record<string, any>;
        include?: string[];
    }): Record<string, any> {
        return { ...opts };
    }

    static readonly builders = {
        tableopts() {
            return new OptsBuild<NonNullable<Parameters<typeof MssqlDialect.tableopts>[0]>>();
        },
        colopts() {
            return new ColOptsBuilder<NonNullable<Parameters<typeof MssqlDialect.colopts>[1]>>();
        },
        indexopts() {
            return new OptsBuild<NonNullable<Parameters<typeof MssqlDialect.indexopts>[0]>>();
        }
    }

    static readonly types = {
        tinyint: () => "tinyint",
        smallint: () => "smallint",
        int: () => "int",
        bigint: () => "bigint",
        bit: () => "bit",
        decimal: (opts?: { precision?: number; scale?: number }) => {
            if (!opts || !opts.precision) return "decimal";
            if (opts.scale == null) return `decimal(${opts.precision})`
            return `decimal(${opts.precision},${opts.scale})`
        },
        money: () => "money",
        smallmoney: () => "smallmoney",
        float: float("float"),
        double: float("double"),
        date: () => "date",
        time: time("time"),
        datetime: () => "datetime",
        datetime2: time("datetime2"),
        datetimeoffset: time("DATETIMEOFFSET"),
        smalldatetime: () => "smalldatetime",
        char: lentype("char"),
        varchar: lentype("varchar"),
        text: () => "text",
        image: () => "image",
        binary: lentype("binary"),
        varbinary: lentype("varbinary"),
        json: () => "json",
        nchar: lentype("nchar"),
        nvarchar: lentype("nvarchar"),
        ntext: () => "ntext",
    }
}


function float(base: string) {
    return (opts?: { n?: number }) => {
        if (!opts || !opts.n) return base;
        return `${base}(${opts.n})`
    }
}

function time(base: string) {
    return (opts?: { precision?: number }) => {
        if (!opts || !opts.precision) return base;
        return `${base}(${opts.precision})`
    }
}

function lentype(base: string) {
    return (opts?: { length?: number }) => {
        if (!opts || !opts.length) return base;
        return `${base}(${opts.length})`
    }
}
