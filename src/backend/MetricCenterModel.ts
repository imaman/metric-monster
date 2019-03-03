import { MetricValue, MetricType, MetricCenterRecord, } from './MetricFactory'

export class Datapoint {
    constructor (public readonly metricName: string, public readonly timestamp: number, public readonly metricValue: any, public readonly absolute?: number) {}
}

abstract class Entry {
    protected data: number[] = [];
    protected value = 0;
    protected numSamples = 0;
    protected fromTimestamp = 0;
    protected readonly sampleSize;
    protected countByValue = new Map<string, number>();
    protected values = new Set<string>();
    private numUpdates = 0;
    private numDatapoints = 0;
    private lastDatapoint = '';
    private lastUpdate = '';
    private lastDatapointNumUpdates = 0;

    constructor(public readonly metricName: string, protected readonly type: MetricType, sampleSize: number,
        protected readonly bufferSize: number) {
        if (sampleSize > 0) {
            this.sampleSize = sampleSize;
        } else if (type == MetricType.PERCENTILE || type === MetricType.PERCENTILE_BOTTOM) {
            this.sampleSize = 1000;
        } else {
            this.sampleSize = 1000;
        }
    }

    public summarize() {
        return {
            type: MetricType[this.type],
            metricName: this.metricName,
            numUpdates: this.numUpdates,
            lastUpdate: this.lastUpdate,
            numDatapoints: this.numDatapoints,
            lastDatapoint: this.lastDatapoint,
            lastDatapointNumUpdates: this.lastDatapointNumUpdates
        }
    }

    updateEntry(n: number, s: string, timestamp: number, labels?: {[s: string]: string}): Datapoint|null {
        this.numUpdates += 1;
        this.lastUpdate = new Date().toISOString();
        const ret = this.updateEntryImpl(n, s, timestamp, labels);
        this.recordDatapoint(ret);
        return ret;
    }

    recordDatapoint(datapoint) {
        if (!datapoint) {
            return;
        }

        this.numDatapoints += 1;
        this.lastDatapoint = new Date().toISOString();
        this.lastDatapointNumUpdates = this.numUpdates;
    }

    public abstract generateDatapoint(timestamp: number): Datapoint|null;
    protected abstract updateEntryImpl(n: number, s: string, timestamp: number, labels?: {[s: string]: string}): Datapoint|null;
}

class GaugeEntry extends Entry {
    protected updateEntryImpl(n: number, s: string, timestamp: number, labels?: {[s: string]: string}) {
        this.value = n;
        const shouldReport = (++this.numSamples % this.sampleSize) === 0;
        if (!shouldReport) {
            return null;
        }

        return this.generateDatapoint(timestamp);
    }

    public generateDatapoint(timestamp: number) {
        this.numSamples = 0;
        return new Datapoint(this.metricName, timestamp, this.value);
    }
}

class RateEntry extends Entry {
    protected updateEntryImpl(n: number, s: string, timestamp: number, labels?: {[s: string]: string}) {
        if (this.numSamples === 0) {
            this.fromTimestamp = timestamp;
        }
        this.value += n;
        this.numSamples += 1;
        if (this.numSamples !== this.sampleSize) {
            return null;
        }
        return this.generateDatapoint(timestamp);
    }

    public generateDatapoint(timestamp: number) {
        const dt = (timestamp - this.fromTimestamp) / 1000;
        if (dt === 0) {
            return null;
        }
        const dvdt = Number((this.value / dt).toFixed(5));
        const ret = new Datapoint(this.metricName, timestamp, dvdt, this.value);
        this.numSamples = 0;
        this.value = 0;
        return ret;
    }
}


class PartitioningEntry extends Entry {
    protected updateEntryImpl(n: number, s: string, timestamp: number, labels?: {[s: string]: string}) {
        if (!s) {
            throw new Error(`a PARTITIONING value (metric name: "${this.metricName}") cannot be falsy`);
        }

        if (!this.values.has(s)) {
            if (this.values.size >= this.bufferSize) {
                s = 'others';
            }
            this.values.add(s);
        }
        const curr: number = this.countByValue.get(s) || 0;
        this.countByValue.set(s, curr + 1);
        this.numSamples += 1;
        if (this.numSamples < this.sampleSize) {
            return null;
        }

        return this.generateDatapoint(timestamp);
    }

    public generateDatapoint(timestamp: number) {
        const d = {};
        this.countByValue.forEach((v, k) => {
            d[k] = v / this.numSamples;
        });
        const ret = new Datapoint(this.metricName, timestamp, d, this.numSamples);

        this.countByValue.clear();
        this.numSamples = 0;
        this.values.clear();

        return ret;
    }
}

class PercentileEntry extends Entry {
    protected updateEntryImpl(n: number, s: string, timestamp: number, labels?: {[s: string]: string}) {
        this.data.push(n);

        if (this.data.length < this.sampleSize) {
            return null;
        }

        return this.generateDatapoint(timestamp);
    }

    public generateDatapoint(timestamp: number) {
        const copy = this.data;
        this.data = [];
        copy.sort((lhs, rhs) => lhs - rhs);
        
        const at = (p: number) => copy[Math.trunc(this.sampleSize * p)]
        const dataToWrite = this.type == MetricType.PERCENTILE
            ? {p10: at(0.1), p50: at(0.5), p90: at(0.9), p99: at(0.99), max: copy[this.sampleSize - 1]}
            : {p90: at(0.9), p50: at(0.5), p10: at(0.1), p1: at(0.01), min: copy[0]};

        return new Datapoint(this.metricName, timestamp, dataToWrite);    
    }
}

class Stopwatch {
    private readonly t0 = Date.now();

    timeInSeconds() {
        const dt = Date.now() - this.t0;
        return Math.trunc(dt / 1000);
    }
}



export class MetricCenterModel {
    private readonly entryByMetricName = new Map<string, Entry>();
    private numRecords = 0;

    public constructor(private readonly stopwatch: Stopwatch = new Stopwatch()) {}

    public summarize(requestedMetricNames) {
        const metricNames = [...this.entryByMetricName.keys()];
        metricNames.sort();

        const entries = metricNames.map(s => this.entryByMetricName.get(s)).filter(Boolean) as Entry[];
        const metrics = entries.filter(curr => !requestedMetricNames.length || requestedMetricNames.includes(curr.metricName))
            .map(curr => curr.summarize());
            
        return {
            metrics,
            numRecords: this.numRecords,
            uptimeInSeconds: this.stopwatch.timeInSeconds(),
        }
    }
    private getEntryByName(metricName: string): Entry|undefined {
        return this.entryByMetricName.get(metricName);
    }

    private getEntry(rec: MetricCenterRecord): Entry {
        let e = this.getEntryByName(rec.metricName);
        if (e) {
            return e;
        }

        if (!rec.type) {
            throw new Error('Falsy type. rec=' + JSON.stringify(rec));
        }

        if (rec.type === MetricType.GAUGE) {
            e = new GaugeEntry(rec.metricName, rec.type, rec.sampleSize || -1, rec.bufferSize || -1);
        } else if (rec.type === MetricType.RATE) {
            e = new RateEntry(rec.metricName, rec.type, rec.sampleSize || -1, rec.bufferSize || -1);
        } else if (rec.type === MetricType.PARTITIONING) {
            e = new PartitioningEntry(rec.metricName, rec.type, rec.sampleSize || -1, rec.bufferSize || -1);
        } else if (rec.type === MetricType.PERCENTILE_BOTTOM || rec.type === MetricType.PERCENTILE) {
            e = new PercentileEntry(rec.metricName, rec.type, rec.sampleSize || -1, rec.bufferSize || -1);
        } else {
            throw new Error(`Unrecognized type (${rec.type})`);
        }

        this.entryByMetricName.set(rec.metricName, e);
        return e;
    }

    put(rec: MetricCenterRecord): Datapoint[] {
        this.numRecords += 1;
        if (!rec.values) {
            const dp = this.putImpl(rec, rec.metricValue);
            if (!dp) {
                return [];
            }

            return [dp];
        }

        const ret: Datapoint[] = [];
        for (const curr of rec.values) {
            const dp = this.putImpl(rec, curr);
            if (dp) {
                ret.push(dp);
            }
        }

        return ret;
    }

    private putImpl(rec: MetricCenterRecord, metricValue: MetricValue|undefined): Datapoint|null {
        if (metricValue === undefined) {
            return null;
        }
        const nValue: number = typeof(metricValue) === 'number' ? metricValue : Number.NaN;
        const sValue: string = typeof(metricValue) === 'string' ? metricValue : '';

        return this.getEntry(rec).updateEntry(nValue, sValue, rec.timestamp, rec.labels);
    }

    flush(metricName: string) {
        const entry = this.getEntryByName(metricName);
        if (!entry) {
            return null;
        }

        const ret = entry.generateDatapoint(Date.now());
        entry.recordDatapoint(ret);
        return ret;
    }

    inspect(name: string): Entry {
        return this.entryByMetricName[name];
    }
}
