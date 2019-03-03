import {Polyvalue} from './Polyvalue'

export class TimedRecord {
    constructor(readonly timestamp: number, readonly value: Polyvalue, readonly absolute: Polyvalue) {}

    static parse(input: any): TimedRecord {
        return new TimedRecord(input.t, Polyvalue.parse(input.v), Polyvalue.parse(input.a));
    }

    withValue(value: Polyvalue): TimedRecord {
        return new TimedRecord(this.timestamp, value, this.absolute);
    }

    toPojo(): any {
        return {t: this.timestamp, v: this.value.toPojo(), a: this.absolute.toPojo()};
    }

    toString() {
        return `${this.constructor.name}: <t=${this.timestamp}, v=${this.value}, a=${this.absolute}>`;
    }
}

