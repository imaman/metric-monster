import {Timeframe, Options, TimedStreamMapper} from './DataTypes';
import { MetricType } from './MetricFactory'
import { Polyvalue, NULL_POLYVALUE } from './Polyvalue';
import { TimedRecord } from './TimedRecord';



function pojosToRecord(options: Options, recordPojos: any[]) {
    if (!options.polyvalue) {
        recordPojos = recordPojos.map(curr => Object.assign({}, curr, {a: (curr.a !== undefined)? {DEFAULT: curr.a} : undefined, v: {DEFAULT: curr.v}}));
    }
    return recordPojos.map(curr => TimedRecord.parse(curr));
}

export class TimedStream {
    private constructor(private readonly records: TimedRecord[], readonly options: Options, private readonly timeframe: Timeframe, private readonly metricType: MetricType|undefined) {
        for (let i = 1; i < records.length; ++i) {
            let prev = records[i - 1];
            let curr = records[i];
            if (curr.timestamp <= prev.timestamp) {
                throw new Error(`Data was supposed to be sorted by timestamp, but it is not (index=${i})`);
            }
        }
    }

    static parse(recordPojos: any[], options: Options, timeframe: Timeframe, metricType: MetricType|undefined) {
        return new TimedStream(pojosToRecord(options, recordPojos), options, timeframe, metricType);
    }

    static toFunction(mapperDescription: TimedStreamMapper|undefined) {
        if (!mapperDescription) {
            return null;
        }

        if (mapperDescription === TimedStreamMapper.MIN_MAX) {
            return minMax;
        }

        return null;
    }

    combine(that: TimedStream, combiner) {
        function aux(recordA: TimedRecord): Polyvalue {
            const vb = that.computeValueAt(recordA.timestamp);
            if (vb.isNull) {
                return NULL_POLYVALUE;
            }

            if (!recordA.value) {
                throw new Error('Found a falsy value');
            }
            return recordA.value.combine(vb, combiner);
        }
        
        const temp: TimedRecord[] = this.records
            .map(tr => tr.withValue(aux(tr)))
            .filter(tr => !tr.value.isNull);
        return this.withRecords(temp);
    }

    mapAll(generator: Mapper): TimedRecord[] {
        return generator(this.computeNames(), this.records, (this.timeframe.fromTimestamp + this.timeframe.toTimestamp) / 2, 
            (this.timeframe.toTimestamp - this.timeframe.fromTimestamp));
    }

    mapIntervals(mapper: Mapper, requireAField = true): TimedStream {
        const millisPerPoint = this.options.datapointIntervalMillis || -1;
        if (millisPerPoint < 0) {
            return this;
        }
    
        // If .a (original absolute value) is not present in all entries, do not recalculate.
        const badEntries = this.filter(curr => curr.absolute.isNull);
        if (requireAField && badEntries.length) {
            return this;
        }

        const outputRecords: TimedRecord[] = [];
        let readIndex = 0;
        let names = this.computeNames();
    
        const adjustedStartTimestamp = this.timeframe.fromTimestamp - this.timeframe.fromTimestamp % millisPerPoint;
        for (let intervalIndex = 0; true; ++intervalIndex) {
            const currentStart = adjustedStartTimestamp + millisPerPoint * intervalIndex;
            const currentInterval = {fromTimestamp: currentStart, toTimestamp: currentStart + millisPerPoint};
            if (currentInterval.toTimestamp > this.timeframe.toTimestamp) {                
                return this.withRecords(outputRecords);
            }
        
            const recordsInInterval: TimedRecord[] = [];
            while(readIndex < this.count()) {
                const curr = this.get(readIndex);
                
                if (isBeforeTimeframe(curr.timestamp, currentInterval)) {
                    ++readIndex;
                    continue;
                }
    
                if (isAfterTimeframe(curr.timestamp, currentInterval)) {
                    break;
                }

                recordsInInterval.push(curr);
                ++readIndex;
            }

            let calculatedTimestamp = currentStart + millisPerPoint / 2;
            if (intervalIndex === 0 && calculatedTimestamp < this.timeframe.fromTimestamp) {
                calculatedTimestamp = this.timeframe.fromTimestamp;
            }

            const newRecords = mapper(names, recordsInInterval, calculatedTimestamp, millisPerPoint);
            outputRecords.push(...newRecords);
        }
    }   

    toPojo() {
        const keys = this.computeNames();
        const ks = {};
        const sigma = {};        
        keys.forEach(s => {
            ks[s] = [];
            sigma[s] = 0;
        });

        this.records.forEach(record => {
            keys.forEach(k => {
                if (!record.value) {
                    throw new Error('bad record: ' + JSON.stringify(record));
                }
                const v = record.value.get(k, Number.NaN);
                ks[k].push(Number.isFinite(v) ? Number(v.toFixed(5)) : v);

                if (!record.absolute.isNull) {
                    const a = record.absolute.get(k, Number.NaN);
                    if (Number.isFinite(a)) {
                        sigma[k] += a;
                    }    
                }
            });
        });
    
        return {
            timestamps: this.records.map(record => record.timestamp),
            sigma,
            values: ks    
        };
    }    

    private withRecords(records: TimedRecord[]) {
        return new TimedStream(records, this.options, this.timeframe, this.metricType);
    }

    private get(index): TimedRecord {
        if (index < 0 || index >= this.records.length) {
            throw new Error(`index (${index}) is out of range [0..${this.records.length})`);
        }

        return this.records[index];
    }

    private count(): number {
        return this.records.length;
    }

    private filter(pred): TimedRecord[] {
        let ret: TimedRecord[] = [];
        for (let i = 0; i < this.count(); ++i) {
            const curr = this.get(i);
            if (pred(curr)) {
                ret.push(curr);
            }
        }

        return ret;
    }

    private computeNames() {
        if (!this.options.polyvalue) {
            return ['DEFAULT'];
        } 
        if (!this.options.polyvalue.length) {
            const set = new Set<string>();
            this.records.forEach(record => {
                record.value.names.forEach(k => set.add(k));
            });
    
            return [...set.values()];
        } 
    
        return [...this.options.polyvalue];
    }

    private computeValueAt(timestamp): Polyvalue {
        const record = this.records.find(curr => curr.timestamp === timestamp);
        if (record) {
            return record.value;
        }
    
        const index = this.records.findIndex(curr => curr.timestamp > timestamp);
        if (index <= 0) {
            // no record was found, or an record was found but at position 0 (=> there is no preceeding record => we cannot
            // predict the value).
            return NULL_POLYVALUE;
        }
    
        const record0 = this.get(index - 1);
        const t0 = record0.timestamp;
        const v0 = record0.value;
    
        const record1 = this.get(index);
        const t1 = record1.timestamp;
        const v1 = record1.value;
        
        return v0.combine(v1, ((v0k, v1k) => {
            const dvdt = (v1k - v0k) / (t1 - t0);
            return v0k + dvdt * (timestamp - t0);
        }));
    }
}

function isBeforeTimeframe(timestamp: number, timeframe: Timeframe) {
    return timestamp < timeframe.fromTimestamp;
}

function isAfterTimeframe(timestamp: number, timeframe: Timeframe) {
    return timestamp >= timeframe.toTimestamp;
}


// const dv = new Map<string, number>();
// names.forEach(curr => dv.set(curr, 0));

// records.forEach(curr => {        
//     names.forEach(k => {
//         const incBy = curr.absolute.get(k, 0);
//         const amount = dv.get(k) || 0;
//         dv.set(k, amount + incBy);
//     });
// });


// const intervalInSecs = intervalInMillis / 1000.0;
// const objV = {};
// dv.forEach((v, k) => objV[k] = normalize(v / intervalInSecs));

// const objA = {};
// dv.forEach((v, k) => objA[k] = v);
// return [new TimedRecord(timestamp, Polyvalue.parse(objV), Polyvalue.parse(objA))];

function minMax(names: string[], records: TimedRecord[], timestamp: number, intervalInMillis: number): TimedRecord[] {
    if (!records.length) {
        return [];
    }

    if (names.length !== 1) {
        return [];
    }

    if (names[0] !== 'DEFAULT') {
        return [];
    }

    if (records.length === 1) {
        return records;
    }

    let min = Number.POSITIVE_INFINITY;
    let minTimestamp = -1;
    let max = Number.NEGATIVE_INFINITY;
    let maxTimestamp = -1;

    let total = records[0].absolute.get('');

    for (let i = 1; i < records.length; ++i) {
        const prev = records[i - 1];
        const curr = records[i];

        const dt = curr.timestamp - prev.timestamp;
        const val = curr.absolute.get('');
        total += val;
        const dv = 1000 * val / dt;
        if (dv < min) {
            min = dv;
            minTimestamp = curr.timestamp;
        } 
        if (dv > max) {
            max = dv;
            maxTimestamp = curr.timestamp;
        } 
    }

    if (maxTimestamp === minTimestamp) {
        return [new TimedRecord(minTimestamp, Polyvalue.parse(min), Polyvalue.parse(0))];
    }


    const ret = [
        new TimedRecord(minTimestamp, Polyvalue.parse(min), Polyvalue.parse(0)),
        new TimedRecord(maxTimestamp, Polyvalue.parse(max), Polyvalue.parse(total))];

    if (ret[0].timestamp > ret[1].timestamp) {        
        const temp = ret[0];
        ret[0] = ret[1];
        ret[1] = temp;
    }

    return ret;
}

export type Mapper = (names: string[], records: TimedRecord[], timestamp: number, intervalInMillis: number) => TimedRecord[];
