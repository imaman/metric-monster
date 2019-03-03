import * as AWS from 'aws-sdk';
import { Uuid, MetricValue, MetricType, MetricCenterRecord, Metric } from './MetricFactory'
import { selfMetrics } from './metricCenterSelfMetrics';
import { AbstractController } from './AbstractController';
import { MetricCenterModel, Datapoint } from './MetricCenterModel'

export abstract class AbstractMetricCenterController extends AbstractController<any, any> {

    private readonly metrics: Metric<MetricValue>[] = [];

    constructor(mapping, buildFingerprint) {        
        super(mapping, buildFingerprint);

        this.metrics.push(...this.computeMetrics());
        this.metrics.push(...selfMetrics.factory.metrics());
    }

    abstract computeMetrics(): Metric<MetricValue>[];

    protected async compute(event: any) {
        selfMetrics.lambdaBuildMetricCenter.put(this.buildFingerprint);
        return super.compute(event);
    }

    async executeInputEvent(event) {
        if (event.inspect) {
            return processInspectRequest(event);
        }
    
        return await processRecordDataRequest(event, this.mapping);
    }

    async executeScheduledEvent() {
        const metricToFlush: string[] = this.metrics.filter(m => m.type === MetricType.RATE).map(m => m.name());

        const datapoints = metricToFlush.map(curr => model.flush(curr));
        selfMetrics.metricCenterNumScheduledDatapoints.put(datapoints.length);
        await writeToStorage([], model, datapoints, this.mapping);
    }
}

async function putDatapoint(mapping, datapoint: Datapoint) {
    const client = new AWS.DynamoDB.DocumentClient({region: mapping.datapointsTable.region});

    let item: any = {
        n: datapoint.metricName,
        t: datapoint.timestamp,
        v: datapoint.metricValue,
    }

    if (datapoint.absolute !== undefined) {
        item.a = datapoint.absolute;
    }

    const putReq = {
        TableName: mapping.datapointsTable.name,
        Item: item
    };    
    try {
        return await client.put(putReq).promise();
    } catch (e) {
        console.log('DB put failed. req=' + JSON.stringify(putReq));
        throw new Error(`put into DB failed. metricName=${datapoint.metricName}. Cause: ${e.message}`);
    }
}



const model = new MetricCenterModel();
let age = 0;
const processedIds = new Set<Uuid>();

async function processRecordDataRequest(event, mapping) {
    const data = event.Records
        .map(r => JSON.parse(new Buffer(r.kinesis.data, 'base64').toString()));

    const records: MetricCenterRecord[] = [];

    let numDups = 0;
    data.forEach((arr: MetricCenterRecord[]) => {
        arr.forEach(r => {
            if (processedIds.has(r.id)) {
                ++numDups;
                return;
            }
    
            records.push(r);
            processedIds.add(r.id);    
        });
    });

    
    const oldestRecord = Math.min(...records.map(r => r.timestamp));
    selfMetrics.metricCenterOldestRecordAge.put(Date.now() - oldestRecord);

    selfMetrics.lambdaInvocationMetricCenter.put(1);
    selfMetrics.metricCenterLambdaAge.put(++age);
    selfMetrics.metricCenterNumRecords.put(event.Records.length);
    selfMetrics.metricCenterDups.put(numDups);
    selfMetrics.metricCenterDedupBufferSize.put(processedIds.size);

    records.push(...selfMetrics.factory.exportAndReset());

    await writeToStorage(records, model, [], mapping);
}

async function writeToStorage(records: MetricCenterRecord[], model: MetricCenterModel, additionalDatapoints: (Datapoint|null)[], mapping: any) {
    const unflattened = records.map((r: MetricCenterRecord) => model.put(r));
    const datapoints = ([] as (Datapoint|null)[]).concat(additionalDatapoints, ...unflattened)
        .filter(Boolean) as Datapoint[];
    if (!datapoints.length) {
        return;
    }

    await Promise.all(datapoints.map(datapoint => {
        return putDatapoint(mapping, datapoint);
    }));
}

function processInspectRequest(event) {
    return model.summarize(event.inspect)
}

