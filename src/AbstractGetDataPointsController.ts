import * as AWS from 'aws-sdk';
import { QueryInput } from 'aws-sdk/clients/dynamodb'
import { MetricType } from './MetricFactory'
import { TimedStream, Mapper } from './TimedStream'
import { Timeframe, Query, Options, Formula, TimedStreamMapper } from './DataTypes';
import { TimedRecord } from './TimedRecord'
import { Polyvalue, NULL_POLYVALUE } from './Polyvalue'
import * as percentile from 'percentile'
import { AbstractController } from './AbstractController'


const DEFAULT_OPTIONS: Options = {
    datapointIntervalMillis: -1
}

export interface Input {
    timeframe: Timeframe,
    queries: Array<Query>
}

export class AbstractGetDataPointsController extends AbstractController<Input, any> {
    private readonly client;
    private readonly tableName: string;
    protected readonly typeByMetricName = new Map<string, MetricType>();
    protected readonly mapperByMetricName = new Map<string, Mapper>();
     
    constructor(mapping, buildFingerprint) {
        super(mapping, buildFingerprint);
        this.client = new AWS.DynamoDB.DocumentClient({
            region: mapping.datapointsTable.region,
            maxRetries: 2
        });
        this.tableName = mapping.datapointsTable.name;
        this.populateMetricMetadata(this.typeByMetricName, this.mapperByMetricName);
    }

    executeScheduledEvent() {
        throw new Error('Not supported');
    }

    executeInputEvent(input: Input) { 
        return Promise.all(input.queries.map(q => this.getDatapoints(input.timeframe, q)));
    }

    protected populateMetricMetadata(types: Map<string, MetricType>, mappers: Map<string, Mapper>) {}    
    
    async fetchData(queryReq: QueryInput) {
        const ret: any[] = [];
        while (true) {
            const resp = await this.client.query(queryReq).promise();
            resp && (resp.Items || []).forEach(item => {
                ret.push(item);
            });
            if (!resp.LastEvaluatedKey) {
                return ret;
            }
    
            queryReq.ExclusiveStartKey = resp.LastEvaluatedKey;
        }
    }

    private async fetchAndRecalculate(metricName: string, timeframe?: Timeframe, options?: Options): Promise<TimedStream> {
        if (!timeframe) {
            throw new Error('Missing timeframe');
        }

        const queryReq = this.computeQueryReq(metricName, timeframe);
        const rawData = await this.fetchData(queryReq);

        const stream = TimedStream.parse(rawData, options || DEFAULT_OPTIONS, timeframe, this.typeByMetricName.get(metricName));

        const metricType = this.typeByMetricName.get(metricName);
        const mapper = this.mapperByMetricName.get(metricName);

        if (mapper) {
            return stream.mapIntervals(mapper);
        }

        try {
            if (metricType === MetricType.RATE) {
                const f = TimedStream.toFunction(TimedStreamMapper.MIN_MAX);
                if (!f) {
                    throw new Error('exepcted to be truthy');
                }

                return stream.mapIntervals(f);
            }

            if (metricType === MetricType.PARTITIONING) {
                return stream.mapIntervals(partitioningMapper);
            }

            if (metricType === MetricType.PERCENTILE || metricType === MetricType.PERCENTILE_BOTTOM) {
                return stream.mapIntervals(percentileMapper, false);
            }

            if (metricType === MetricType.GAUGE) {
                return stream.mapIntervals(gaugeMapper, false);
            }
        } catch (e) {
            e.message = `(details: metricName=${metricName}, type=${metricType && MetricType[metricType]}) ${e.message}`;
            throw e;
        }

        return stream;
    }

    private computeQueryReq(metricName: string, timeframe: Timeframe) {
        const vals: any = {
            ':n': metricName,
            ':from': timeframe.fromTimestamp,
            ':to': timeframe.toTimestamp
        };  

        const ret: QueryInput = {
            TableName: this.tableName,
            KeyConditionExpression: 'n = :n and (t between :from and :to)',
            ExpressionAttributeValues: vals            
        };

        return ret;
    }

    async getDatapoints(timeframe: Timeframe, query: Query) {
        query.timeframe = query.timeframe || timeframe;

        const pA = this.fetchAndRecalculate(query.metricName, query.timeframe, query.options);
        const pB = query.per ? this.fetchAndRecalculate(query.per.metricName, query.timeframe, query.per.options) : Promise.resolve(null);
        
        const [streamA, streamB] = await Promise.all([pA, pB]);
        
        let stream = streamA;
        const per = query.per;
        if (per) {
            if (!streamB) {
                throw new Error('shuld not be null at this point');
            }

            const formula = per.formula ? Formula[per.formula] : Formula.FRACTION;
            if (!formula) {
                throw new Error('Bad formula value');
            }
            stream = streamA.combine(streamB, (a: number, b: number) => ratioCombiner(formula, a, b));
        }

        const pojo = stream.toPojo();
        const metricType = this.typeByMetricName.get(query.metricName);
        if (metricType === MetricType.PARTITIONING) {
            const sigma = stream.mapAll(partitioningMapper);
            if (sigma.length > 1) { 
                throw new Error(`mapper returned more than one record (${sigma.length})`);
            }
            if (sigma.length === 1) {
                pojo.sigma = sigma[0].value.toPojo();
            }
        }
        if (metricType === MetricType.PERCENTILE || metricType === MetricType.PERCENTILE_BOTTOM || metricType === MetricType.GAUGE) {
            delete pojo.sigma;
        }

        return Object.assign({}, pojo, {query});
    }
}

function ratioCombiner(formula, x1: number, x2: number) {
    if (formula == Formula.FRACTION) {
        return x1 / x2;
    } else if (formula == Formula.FRACTION_COMPLEMENT) {
        return 1.0 - x1 / x2;
    } else if (formula == Formula.PARTS) {
        return x1 / (x1 + x2);
    } else {
        throw new Error(`Unsupported formula value ${formula}`);
    }    
}

    
export function rateMapper(names: string[], records: TimedRecord[], timestamp: number, intervalInMillis: number): TimedRecord[] {
    const dv = new Map<string, number>();
    names.forEach(curr => dv.set(curr, 0));

    records.forEach(curr => {        
        names.forEach(k => {
            const incBy = curr.absolute.get(k, 0);
            const amount = dv.get(k) || 0;
            dv.set(k, amount + incBy);
        });
    });


    const intervalInSecs = intervalInMillis / 1000.0;
    const objV = {};
    dv.forEach((v, k) => objV[k] = normalize(v / intervalInSecs));

    const objA = {};
    dv.forEach((v, k) => objA[k] = v);
    return [new TimedRecord(timestamp, Polyvalue.parse(objV), Polyvalue.parse(objA))];
}

function partitioningMapper(names: string[], records: TimedRecord[], timestamp: number, intervalInMillis: number): TimedRecord[] {
    if (!records.length) {
        return [];
    }

    const dv = new Map<string, number>();
    names.forEach(curr => dv.set(curr, 0));

    let total = 0;
    records.forEach(curr => {
        const a = curr.absolute;
        if (a.count !== 1) {
            throw new Error('Found a PARTITIONING record with .a that is not a singleton');
        }

        const multiplier = a.get('DEFAULT');
        total += multiplier;

        names.forEach(k => {
            const incBy = curr.value.get(k, 0) * multiplier;
            const amount = dv.get(k) || 0;
            dv.set(k, amount + incBy);
        });
    });


    const objV = {};
    dv.forEach((v, k) => objV[k] = normalize(v / total));

    const objA = {DEFAULT: total};
    return [new TimedRecord(timestamp, Polyvalue.parse(objV), Polyvalue.parse(objA))];
}

function percentileMapper(names: string[], records: TimedRecord[], timestamp: number, intervalInMillis: number): TimedRecord[] {
    if (!records.length) {
        return [];
    }

    const dv = new Map<string, number[]>();
    names.forEach(curr => dv.set(curr, []));

    function isOk(name) {
        return name === 'min' || name === 'max' || name.length && name.startsWith("p") && Number.isFinite(Number(name.substr(1)))
    }
    const badNames = names.filter(x => !isOk(x));
    if (badNames.length) {
        throw new Error('Found bad names: ' + badNames.slice(0, 10).join(', '));
    }

    records.forEach(curr => {
        names.forEach(k => {
            const v = curr.value.get(k);
            if (v !== undefined) {
                const arr = dv.get(k);
                if (!arr) {
                    throw new Error('Should not be falsy at this point');
                }
                arr.push(v);
            }    
        });
    });

    const objV = {};    
    function compute(key, func) {
        const arr = dv.get(key);
        if (!arr) {
            return;
        }
        
        const temp = func(...arr);
        if (temp !== undefined) {
            objV[key] = temp;
        }
    }    
    

    names.forEach(k => {
        const n = Number(k.substr(1));
        if (n < 50 || k === 'min') {
            compute(k, Math.min);
        } else if (n > 50 || k === 'max') {
            compute(k, Math.max);
        } else {
            compute(k, (...arr) => percentile(50, arr));
        }
    });

    return [new TimedRecord(timestamp, Polyvalue.parse(objV), NULL_POLYVALUE)];
}

function gaugeMapper(names: string[], records: TimedRecord[], timestamp: number, intervalInMillis: number): TimedRecord[] {

    let chosen: TimedRecord|null = null;
    let minTimeGap = Number.POSITIVE_INFINITY;
    for (const curr of records) {
        const timeGap = Math.abs(curr.timestamp - timestamp);
        if (timeGap < minTimeGap) {
            minTimeGap = timeGap;
            chosen = curr;
        }
    }


    return chosen ? [chosen] : [];
}


function normalize(n: number) {
    return Number(n.toFixed(5));
}
