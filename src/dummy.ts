import type { Fragments } from "./frag.js";
import type { DBContext, Value } from "./types.js";

export const dummydbctx: DBContext = {
    quote: function (name: string): string {
        return `\`${name}\``;
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
};