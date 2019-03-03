import * as AWS from 'aws-sdk';
import { PutRecordInput } from 'aws-sdk/clients/kinesis';
import * as uuid from 'uuid/v1'

export enum MetricType {
    PERCENTILE = 1,
    RATE,
    GAUGE,
    PERCENTILE_BOTTOM,    
    PARTITIONING
}

export type Uuid = string;

export type MetricValue = string|number;

export interface MetricCenterRecord {
    id: Uuid
    metricName: string
    timestamp: number
    values?: MetricValue[]
    metricValue: MetricValue|undefined
    type: MetricType,
    sampleSize?: number
    bufferSize?: number
    labels?: {[s: string]: string}
}

export interface MetricConfig {
    name: string,
    sampleSize: number
    bufferSize?: number
}



export class Metric<T extends MetricValue> {
    private filter = (_: T) => true;
    private readonly metricConfig: MetricConfig;

    constructor(readonly type: MetricType, metricConfig: MetricConfig, readonly factory: MetricFactory) {        
        this.metricConfig = Object.assign({bufferSize: -1}, metricConfig)
        factory.register(this);
    }

    name() {
        return this.metricConfig.name;
    }

    withFilter(arg) {
        this.filter = arg;
        return this;
    }

    public put(value: T, labels?: Map<string, string>) {
        if (!this.filter(value)) {
            return;
        }

        return this.putImpl(value, undefined, labels);
    }

    public putValues(values: T[], labels?: Map<string, string>) {
        const fileted = values.filter(curr => this.filter(curr));
        if (!fileted.length) {
            return;
        }
        return this.putImpl(undefined, fileted, labels);
    }

    private putImpl(metricValue: T|undefined, values: T[]|undefined, labels?: Map<string, string>) {
        labels = labels || new Map<string, string>();

        const labelsObj = {};
        labels.forEach((v, k) => {
            labelsObj[k] = v;
        });
        const timestamp = Date.now();
        const rec: MetricCenterRecord = {
            id: uuid(),
            metricName: this.metricConfig.name,
            timestamp,
            metricValue,
            values,
            type: this.type,
            sampleSize: this.metricConfig.sampleSize,
            bufferSize: this.metricConfig.bufferSize,
            labels: labelsObj
        }
        this.factory.enqueue(rec);
    }
}


export class MetricFactory {
    private recs: MetricCenterRecord[] = [];
    private metricByName = new Map<string, Metric<MetricValue>>();

    constructor(private readonly flushingThreshold = 1) {}

    register(m: Metric<MetricValue>) {
        if (this.metricByName.has(m.name())) {
            throw new Error(`Name conflict on metric name "${m.name()}"`);
        }
        this.metricByName.set(m.name(), m);
    }

    metrics() {
        const ret = [...this.metricByName.values()];
        ret.sort((a, b) => a.name().localeCompare(b.name()));
        return ret;
    }

    enqueue(rec: MetricCenterRecord) {
        this.recs.push(rec);
    }

    exportAndReset() {
        if (this.recs.length < this.flushingThreshold) {
            return [];
        }

        const ret = this.recs;
        this.recs = [];
        return ret;
    }

    async flush(mapping) {
        if (!mapping.metricCenterStream) {
            throw new Error('Missing runtime instrument dependency: metricCenterStream');
        }

        const recs = this.exportAndReset();
        if (!recs.length) {
            return;
        }
        const kinesis = new AWS.Kinesis({region: mapping.metricCenterStream.region})
        const putReq: PutRecordInput = {
            StreamName: mapping.metricCenterStream.name,
            Data: JSON.stringify(recs),
            PartitionKey: recs[0].metricName
        };

        try {
            await kinesis.putRecord(putReq).promise();           
        } catch (e) {
            // Intentionally absorb.
        }
    }

    newRate(metricConfig: MetricConfig): Metric<number> {
        return new Metric<number>(MetricType.RATE, metricConfig, this);
    }
    
    newPercentile(metricConfig: MetricConfig): Metric<number> {
        return new Metric<number>(MetricType.PERCENTILE, metricConfig, this);
    }
    
    newPercentileBottom(metricConfig: MetricConfig): Metric<number> {
        return new Metric<number>(MetricType.PERCENTILE_BOTTOM, metricConfig, this);
    }
    
    newGauge(metricConfig: MetricConfig): Metric<number> {
        return new Metric<number>(MetricType.GAUGE, metricConfig, this);
    }
    
    newPartitioning(metricConfig: MetricConfig): Metric<string> {
        return new Metric<string>(MetricType.PARTITIONING, metricConfig, this);
    }    

    static factoriesOf(...metrics: Metric<MetricValue>[]) {
        const set = new Set<MetricFactory>();
        for (const curr of metrics) {
            set.add(curr.factory);
        }
        return [...set.values()]
    }
}
