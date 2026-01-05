import type { Schema, ISQLColumn } from "./crud.js";

export class DDLGenerator<Key> {
    private _table: Schema<any, any>;

    constructor(table: Schema<any, any>) {
        this._table = table;
    }

    newcol(newcol: ISQLColumn) { }
    dropcol(col: Key) { }
    modcol(fromcol: Key, tocol: ISQLColumn): void { }
}
