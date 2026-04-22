import type { IDDLColOpts } from "./ddl.js";

export class OptsBuild<T extends Record<string, any>> {
    protected opts: Record<string, any>;

    constructor(init?: T) {
        this.opts = { ...init };
    }

    set(key: keyof T & string, value: T[keyof T]): this {
        this.opts[key] = value;
        return this;
    }

    finish(): T {
        return this.opts as T;
    }
}

export class ColOptsBuilder<E extends Record<string, any>> extends OptsBuild<IDDLColOpts> {
    ext(key: keyof E & string, value: E[keyof E]): this {
        if (!this.opts.extra) this.opts.extra = {};
        this.opts.extra[key] = value;
        return this;
    }
}