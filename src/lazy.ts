import type { Op } from "./op.js";
import type { SqlTable } from "./table.js";
import type { Identifier, RawSql } from "./types.js";
import { Fragments } from "./frag.js";

export const lazy = {
    SqlTable: {} as any as typeof SqlTable,
    Op: {} as any as typeof Op,
    RawSql: {} as any as typeof RawSql,
    Identifier: {} as any as typeof Identifier,
    Fragments: {} as any as typeof Fragments,
};
