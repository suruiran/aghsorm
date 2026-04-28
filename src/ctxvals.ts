import { type Batch } from "./frag.js";
import { lazy } from "./lazy.js";
import { type DBContext } from "./types.js";
import "zone.js";

// TODO: https://github.com/tc39/proposal-async-context

const BatchKey = "aghsorm.batch";
const AddBatchKey = "aghsorm.addbatch";
const DBCtxKey = "aghsorm.dbctx";

type AddBatch<T> = (batch: Batch<T>) => void;

export function runInAddBatch<T>(name: string, add: AddBatch<T>, fnc: () => any) {
    const scope = Zone.current.fork({ name, properties: { [AddBatchKey]: add } });
    return scope.run(fnc);
}

export function batch<T>(name: string, fnc: () => T) {
    const cb = Zone.current.get(BatchKey);
    if (cb) {
        throw new Error(`aghsorm: you are already in a batch scope, ${cb.name}`);
    }
    const batch = new lazy.Batch<T>(name);
    const addbatch = Zone.current.get(AddBatchKey) as AddBatch<T> | undefined;
    if (!addbatch) {
        throw new Error(`aghsorm: you are not in a batch scope.`);
    }
    addbatch(batch);
    const scope = Zone.current.fork({ name: `zoneof: batch ${name}`, properties: { [BatchKey]: batch } });
    batch._fnc = () => {
        return scope.run(fnc);
    }
}

export function getBatch<T>(): Batch<T> | undefined {
    return Zone.current.get(BatchKey);
}

export function runInCtx(name: string, ctx: DBContext, fnc: () => any) {
    const scope = Zone.current.fork({ name, properties: { [DBCtxKey]: ctx } });
    return scope.run(fnc);
}

export function getCtx(): DBContext | undefined {
    return Zone.current.get(DBCtxKey);
}
