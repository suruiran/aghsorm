import { ColOptsBuilder, OptsBuild } from "./builder.js";
import { IDDLColOpts } from "./ddl.js";
import { Fragments } from "./frag.js";
import { QuoteSQLStringLiteral } from "./utils.js";

export class MysqlDialect {
    static readonly ClassName = "MysqlDialect";

    static tableopts(opts?: {
        temp?: boolean;
        ifNotExists?: boolean;
        autoExtendSize?: number | Fragments;
        autoIncrement?: number | Fragments;
        avgRowLength?: number | Fragments;
        characterSet?: string;
        checkSum?: boolean;
        collate?: string;
        comment?: string;
        compression?: "ZLIB" | "LZ4";
        dataDirectory?: string;
        indexDirectory?: string;
        delayKeyWrite?: boolean;
        encryption?: boolean;
        engine?: "InnoDB" | "MyISAM" | "MEMORY" | "CSV" | "ARCHIVE" | "EXAMPLE" | "FEDERATED" | "HEAP" | "MERGE" | "NDB";
        engineAttribute?: string;
        insertMethod?: "FIRST" | "LAST";
        keyBlockSize?: number | Fragments;
        maxRows?: number | Fragments;
        minRows?: number | Fragments;
        packKeys?: boolean;
        password?: string;
        rowFormat?: "DYNAMIC" | "FIXED" | "COMPRESSED" | "REDUNDANT" | "COMPACT";
        secondaryEngineAttribute?: string;
        statsAutoRecalc?: boolean;
        statsPersistent?: boolean;
        statsSamplePages?: number | Fragments;
        tablespace?: {
            name: string;
            kind: "DISK" | "MEMORY";
        }
        union?: string[];

        // CONSTRAINTS:
        check?: {
            frags: Fragments;
            enforced?: boolean;
        }
    }): Record<string, any> {
        return { ...opts };
    }

    static colopts(
        opts: IDDLColOpts,
        extra?: {
            invisible?: boolean;
            collate?: string;
            columnFormat?: "FIXED" | "DYNAMIC";
            engineAttribute?: string;
            secondaryEngineAttribute?: string;
            storage?: "DISK" | "MEMORY";
            check?: {
                frags: Fragments;
                enforced?: boolean;
            };
            as?: Fragments;
        }
    ): Record<string, any> {
        return { ...opts, extra: { ...opts.extra, ...extra } };
    }

    static indexopts(opts?: {
        kind?: "UNIQUE" | "FULLTEXT" | "SPATIAL";
        type?: "BTREE" | "HASH";
        keyBlockSize?: number | Fragments;
        parser?: string;
        comment?: string;
        invisible?: boolean;
        engineAttribute?: string;
        secondaryEngineAttribute?: string;
        algorithm?: "INPLACE" | "COPY";
        lock?: "NONE" | "SHARED" | "EXCLUSIVE";
    }): Record<string, any> {
        return { ...opts };
    }

    static readonly types = {
        bit: (size?: number) => {
            if (!size) return "BIT"
            return `BIT(${size})`
        },
        tinyint: mysqlint("TINYINT"),
        bool: () => "BOOL",
        smallint: mysqlint("SMALLINT"),
        mediumint: mysqlint("MEDIUMINT"),
        int: mysqlint("INT"),
        bigint: mysqlint("BIGINT"),
        decimal: decimal("DECIMAL"),
        float: mysqlfloat("FLOAT"),
        double: mysqlfloat("DOUBLE"),
        date: () => "DATE",
        time: () => "TIME",
        datetime: () => "DATETIME",
        timestamp: () => "TIMESTAMP",
        year: () => "YEAR",
        char: mysqltext("CHAR"),
        varchar: mysqltext("VARCHAR"),
        binary: lentype("BINARY"),
        varbinary: lentype("VARBINARY"),
        tinyblob: () => "TINYBLOB",
        tinytext: (opts?: { charset?: string; collate?: string }) => {
            return mysqltext("TINYTEXT")(opts);
        },
        blob: lentype("BLOB"),
        text: mysqltext("TEXT"),
        mediumblob: () => "MEDIUMBLOB",
        mediumtext: (opts?: { charset?: string; collate?: string }) => {
            return mysqltext("MEDIUMTEXT")(opts);
        },
        longblob: () => "LONGBLOB",
        longtext: (opts?: { charset?: string; collate?: string }) => {
            return mysqltext("LONGTEXT")(opts);
        },
        enum: (opts: { values: string[]; charset?: string; collate?: string }) => {
            return mysqltext(`ENUM(${opts.values.map(v => QuoteSQLStringLiteral(v)).join(",")})`)(opts);
        },
        set: (opts: { values: string[]; charset?: string; collate?: string }) => {
            return mysqltext(`SET(${opts.values.map(v => QuoteSQLStringLiteral(v)).join(",")})`)(opts);
        },
        json: () => "JSON",
    };

    static readonly builders = {
        tableopts() {
            return new OptsBuild<NonNullable<Parameters<typeof MysqlDialect.tableopts>[0]>>();
        },
        colopts() {
            return new ColOptsBuilder<NonNullable<Parameters<typeof MysqlDialect.colopts>[1]>>();
        },
        indexopts() {
            return new OptsBuild<NonNullable<Parameters<typeof MysqlDialect.indexopts>[0]>>();
        }
    }
}

function mysqlint(base: string) {
    return (opts?: { unsigned?: boolean }) => {
        return `${base}${opts?.unsigned ? " UNSIGNED" : ""}`
    }
}

function mysqlfloat(base: string) {
    return (opts?: { precision?: number, unsigned?: boolean }) => {
        const val = decimal(base)({ precision: opts?.precision } as any);
        return `${val}${opts?.unsigned ? " UNSIGNED" : ""}`
    }
}

function lentype(base: string) {
    return (opts?: { length?: number }) => {
        if (!opts || !opts.length) return base;
        return `${base}(${opts.length})`
    }
}


function mysqltext(base: string) {
    return (opts?: { length?: number; charset?: string; collate?: string }) => {
        const val = lentype(base)(opts);
        return `${val}${opts?.charset ? ` CHARACTER SET ${opts.charset}` : ""}${opts?.collate ? ` COLLATE ${opts.collate}` : ""}`
    };
}

function decimal(base: string) {
    return (opts?: { precision?: number; scale?: number; unsigned?: boolean }) => {
        if (!opts || !opts.precision) return base;
        if (opts?.scale == null) return `${base}(${opts?.precision})`;
        return mysqlint(`${base}(${opts?.precision}, ${opts?.scale})`)({ unsigned: opts?.unsigned || false })
    }
}
