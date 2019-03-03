

function classNameOf(o: any) {
    if (!o) {
        return 'NULL'
    }

    if (!o.constructor) {
        return 'NO_CTOR';
    }

    return o.constructor.name;
}

export class Polyvalue {

    protected constructor(private readonly map: Map<string, number>) {
        if (!(map instanceof Map)) {
            throw new Error(`Runtime type mismatch: expected (Map) found(${classNameOf(map)})`);
        }
    }

    static parse(obj: any): Polyvalue {
        if (typeof(obj) === 'number') {
            const map = new Map<string, number>();
            map.set('DEFAULT', obj);
            return new Polyvalue(map);
        }
        if (!obj) {
            return NULL_POLYVALUE;
        }
        const map = new Map<string, number>();
        Object.keys(obj).forEach(k => {
            map.set(k, obj[k]);
        })
        return new Polyvalue(map);
    }

    get count() {
        return this.map.size;
    }

    toString() {
        const data = [...this.map.entries()].map(([x, y]) => `${x}=>${y}`).join(', ');
        return `${this.constructor.name}: <${data}>`;
    }

    get isNull() {
        return false;
    }

    get names() {
        return [...this.map.keys()];
    }

    get(name: string, defaultValue?: number) {
        if (this.count === 1 && this.names[0] === 'DEFAULT') {
            name = 'DEFAULT';
        }
        const ret = this.map.get(name);
        if (ret !== undefined) {
            return ret;
        }

        if (defaultValue !== undefined) {
            return defaultValue;
        }
        
        throw new Error(`No value with the specified name (${name})`);
    }

    combine(that: Polyvalue, combiner) {
        if (!(that instanceof Polyvalue)) {
            throw new Error('Expected a polyvalue');
        }

        if (that.isNull) {
            return that;
        }
        const map = new Map<string, number>();
        this.names.forEach(n => map.set(n, combiner(this.get(n), that.get(n))));

        return new Polyvalue(map);
    }

    toPojo(): any|null {
        const ret = {};
        this.map.forEach((v, k) => ret[k] = v);
        return ret;
    }
}


class NullPolyvalue extends Polyvalue {
    constructor() {
        super(new Map<string, number>());
    }

    get isNull() {
        return true;
    }

    combine(that: Polyvalue, combiner) {
        return this;
    }

    toPojo() {
        return null;
    }
}

export const NULL_POLYVALUE = new NullPolyvalue();