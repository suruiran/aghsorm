import { DBCtxKey } from "./ctxvals.js";
import { Fragments } from "./frag.js";
import type { DBContext, Value } from "./types.js";

export const dummydbctx: DBContext = {
    quote(usecase: "id" | "stringliteral", name: string): string {
        if (usecase === "id") {
            return `\`${name}\``;
        }
        return `'${name.replace(/'/g, "''")}'`;
    },
    register: function (fragments: Fragments) {
        const tmp = [] as string[];
        const args = [] as Value[];
        for (const ele of fragments) {
            if (ele.sql != null) {
                tmp.push(ele.sql);
                continue;
            }
            tmp.push(`\$${args.length + 1}`);
            args.push(ele.value!);
        }
        console.log([tmp.join(" "), args]);
    },
    addtable() {
        return new Fragments();
    },
};

export function runInCtx(name: string, ctx: DBContext, fnc: () => any) {
    const scope = Zone.current.fork({ name, properties: { [DBCtxKey]: ctx } });
    return scope.run(fnc);
}