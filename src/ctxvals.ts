import { type DBContext } from "./types.js";
import "zone.js";

export const DBCtxKey = "aghsorm.dbctx";
export function getCtx(): DBContext | undefined {
    return Zone.current.get(DBCtxKey);
}
