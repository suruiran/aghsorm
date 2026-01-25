import type { SqlTable, ISQLColumn } from "./crud.js";

export class DDLGenerator<Key> {
    private _table: SqlTable<any, any>;

    constructor(table: SqlTable<any, any>) {
        this._table = table;
    }

    newcol(newcol: ISQLColumn) { }
    dropcol(col: Key) { }
    modcol(fromcol: Key, tocol: ISQLColumn): void { }
}
