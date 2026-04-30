import { type DBContext } from "./types.js";
import "zone.js";

export const DBCtxKey = "aghsorm.dbctx";

export function getCtx(): DBContext | undefined {
    return Zone.current.get(DBCtxKey);
}

export function mustdbctx(): DBContext {
    const dbctx = getCtx();
    if (!dbctx) {
        throw new Error("aghsorm: can not find DBContext in Zone.");
    }
    return dbctx;
}