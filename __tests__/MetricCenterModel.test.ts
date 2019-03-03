import * as chai from 'chai';
import chaiSubset = require('chai-subset');

chai.use(chaiSubset);
const {expect} = chai;

import 'mocha';
import { MetricCenterRecord, MetricType } from '../src/MetricFactory'
import { MetricCenterModel } from '../src/MetricCenterModel'


describe('MetricCenterModel', () => {
    describe('gauge', () => {
        it ('tracks data', () => {
            const m = new MetricCenterModel();
            const r: MetricCenterRecord = {
                id: 'a_1',
                metricName: 'n_1',
                sampleSize: 1,
                timestamp: 0,
                metricValue: 100,
                type: MetricType.GAUGE
            };
            const datapoint = m.put(r);
            expect(flattened(datapoint)[0]).to.containSubset({
                metricName: 'n_1',
                metricValue: 100,
                timestamp: 0
            });
        });
        it('generates a datapoint once every <sampleSize> updates', () => {
            const m = new MetricCenterModel();
            const r: MetricCenterRecord = {
                id: 'a_1',
                metricName: 'n_1',
                sampleSize: 5,
                timestamp: 0,
                metricValue: 100,
                type: MetricType.GAUGE
            };
            const datapoints = Array(10).fill(0).map((_, i) => { 
                r.timestamp = i;
                return m.put(r)
            });
            const nonNulls = datapoints.map((d, i) => d.length ? i : -1).filter(x => x >= 0);
            expect(nonNulls).to.eql([4, 9]);
        });
    });

    describe('rate', () => {
        it('computes dv/dt', () => {
            const m = new MetricCenterModel();

            let id = 0;
            function newRateRecord(v, t): MetricCenterRecord {
                return {
                    id: `ID_${++id}`,
                    metricName: 'n_1',
                    sampleSize: 4,
                    bufferSize: 1,
                    timestamp: t,
                    metricValue: v,
                    type: MetricType.RATE
                };    
            }

            m.put(newRateRecord(100, 0));
            m.put(newRateRecord(40, 15));
            m.put(newRateRecord(90, 16));
            const datapoint = m.put(newRateRecord(10, 30));

            const MILLISEC_TO_SEC_FACTOR = 1000;

            expect(flattened(datapoint)[0]).to.containSubset({
                metricValue: 8 * MILLISEC_TO_SEC_FACTOR,
                absolute: 240
            });
        });
        it('computes dv/dt for each datapoint in isolation', () => {
            const m = new MetricCenterModel();

            let id = 0;
            function newRateRecord(v: number, t: number): MetricCenterRecord {
                return {
                    id: `ID_${++id}`,
                    metricName: 'n_1',
                    sampleSize: 2,
                    bufferSize: 1,
                    timestamp: t,
                    metricValue: v,
                    type: MetricType.RATE
                };    
            }
            
            expect(m.put(newRateRecord(90, 100))).to.eql([]);
            expect(m.put(newRateRecord(150, 300))).to.eql([{
                absolute: 240,
                metricValue: 1200,
                metricName: 'n_1',
                timestamp: 300
            }]);

            expect(m.put(newRateRecord(25, 350))).to.eql([]);
            expect(m.put(newRateRecord(45, 850))).to.eql([{
                absolute: 70,
                metricValue: 140,
                metricName: 'n_1',
                timestamp: 850
            }]);
        });
        it('generates a datapoint once every <sampleSize> updates', () => {
            const m = new MetricCenterModel();
            const r: MetricCenterRecord = {
                id: 'a_1',
                metricName: 'n_1',
                sampleSize: 6,
                timestamp: 0,
                metricValue: 20,
                type: MetricType.RATE
            };
            const datapoints = Array(33).fill(0).map((_, i) => { 
                r.timestamp = i;
                return m.put(r)
            });
            const nonNulls = datapoints.map((d, i) => d.length ? i : -1).filter(x => x >= 0);
            expect(nonNulls).to.eql([5, 11, 17, 23, 29]);
        });
    });

    describe('percentiles', () => {
        it ('generates a datapoint with values for 10, 50, 90, 99 percentiles (and max)', () => {
            const m = new MetricCenterModel();
            const r: MetricCenterRecord = {
                id: 'a_1',
                metricName: 'n_1',
                sampleSize: 20,
                timestamp: 0,
                metricValue: 100,
                type: MetricType.PERCENTILE
            };

            let datapoint;
            for (let i = 0; i < 20; ++i) {
                r.id = `id_${i}`;
                r.metricValue = 2500 - (i + 1) * 5;
                datapoint = m.put(r);
            }
            expect(flattened(datapoint)[0]).to.containSubset({
                metricName: 'n_1',
                timestamp: 0,
                metricValue: {
                    max: 2495,
                    p10: 2410,
                    p50: 2450,
                    p90: 2490,
                    p99: 2495
                }
            });
        });
        it ('auto-clears between datapoints', () => {
            const m = new MetricCenterModel();
            const r: MetricCenterRecord = {
                id: 'a_1',
                metricName: 'n_1',
                sampleSize: 2,
                timestamp: 0,
                metricValue: 0,
                type: MetricType.PERCENTILE
            };

            r.metricValue = 9;
            expect(m.put(r)).to.eql([]);
            r.metricValue = 8;
            expect(m.put(r)[0]).to.containSubset({metricValue: {max: 9}});

            r.metricValue = 7;
            expect(m.put(r)).to.eql([]);
            r.metricValue = 6;
            expect(m.put(r)[0]).to.containSubset({metricValue: {max: 7}});
        });
    });

    describe('percentiles bottom', () => {
        it ('generates a datapoint with values for 1, 10, 50, 90 percentiles (and min)', () => {
            const m = new MetricCenterModel();
            const r: MetricCenterRecord = {
                id: 'a_1',
                metricName: 'n_1',
                sampleSize: 20,
                timestamp: 0,
                metricValue: 100,
                type: MetricType.PERCENTILE_BOTTOM
            };

            let datapoint;
            for (let i = 0; i < 20; ++i) {
                r.id = `id_${i}`;
                r.metricValue = 2500 - (i + 1) * 5;
                datapoint = m.put(r);
            }
            expect(flattened(datapoint)[0]).to.containSubset({
                metricName: 'n_1',
                timestamp: 0,
                metricValue: {
                    min: 2400,
                    p1: 2400,
                    p10: 2410,
                    p50: 2450,
                    p90: 2490,
                }
            });
        });
        it ('auto-clears between datapoints', () => {
            const m = new MetricCenterModel();
            const r: MetricCenterRecord = {
                id: 'a_1',
                metricName: 'n_1',
                sampleSize: 2,
                timestamp: 0,
                metricValue: 0,
                type: MetricType.PERCENTILE_BOTTOM
            };

            r.metricValue = 1;
            expect(m.put(r)).to.eql([]);
            r.metricValue = 2;
            expect(m.put(r)[0]).to.containSubset({metricValue: {min: 1}});

            r.metricValue = 3;
            expect(m.put(r)).to.eql([]);
            r.metricValue = 4;
            expect(m.put(r)[0]).to.containSubset({metricValue: {min: 3}});
        });
    });

    describe('partitioning', () => {
        it('rejects falsy values', () => {
            const m = new MetricCenterModel();
            const r: MetricCenterRecord = {
                id: 'a_1',
                metricName: 'n_1',
                sampleSize: 20,
                bufferSize: 20,
                timestamp: 0,
                metricValue: "",
                type: MetricType.PARTITIONING
            };

            expect(() => m.put(r)).to.throw('a PARTITIONING value (metric name: "n_1") cannot be falsy');
        });
        it ('tracks data', () => {
            const m = new MetricCenterModel();
            const r: MetricCenterRecord = {
                id: 'a_1',
                metricName: 'n_1',
                sampleSize: 20,
                bufferSize: 20,
                timestamp: 0,
                metricValue: 100,
                type: MetricType.PARTITIONING
            };

            const fruit = ['apple', 'banana', 'orange'];
            let datapoint;
            for (let i = 0; i < 20; ++i) {
                const v = i % 5 < 2 ? fruit[i % 5] : fruit[2];

                r.id = `id_${i}`;
                r.metricValue = v;
                datapoint = m.put(r);
            }
            expect(flattened(datapoint)[0]).to.containSubset({
                metricName: 'n_1',
                timestamp: 0,
                metricValue: {
                    apple: 0.2,
                    banana: 0.2,
                    orange: 0.6
                }
            });
        });
        it ('stores number of events', () => {
            const m = new MetricCenterModel();
            const r: MetricCenterRecord = {
                id: 'a_1',
                metricName: 'n_1',
                sampleSize: 20,
                bufferSize: 20,
                timestamp: 0,
                metricValue: 100,
                type: MetricType.PARTITIONING
            };

            const fruit = ['apple', 'banana', 'orange'];
            let datapoint;
            for (let i = 0; i < 20; ++i) {
                const v = Boolean(i % 2) ? 'ODD' : 'EVEN';

                r.id = `id_${i}`;
                r.metricValue = v;
                datapoint = m.put(r);
            }
            expect(flattened(datapoint)[0]).to.containSubset({
                absolute: 20
            });
        });
        it ('auto clears', () => {
            const m = new MetricCenterModel();
            const r: MetricCenterRecord = {
                id: 'a_1',
                metricName: 'n_1',
                sampleSize: 20,
                bufferSize: 20,
                timestamp: 0,
                metricValue: 100,
                type: MetricType.PARTITIONING
            };

            const fruit = ['apple', 'banana', 'orange'];
            for (let i = 0; i < 20; ++i) {
                const v = i % 5 < 2 ? fruit[i % 5] : fruit[2];
                
                r.id = `id_a_${i}`;
                r.metricValue = v;
                m.put(r);
            }
            let datapoint;
            for (let i = 0; i < 20; ++i) {
                r.id = `id_b_${i}`;
                r.metricValue = 'mango'
                datapoint = m.put(r);
            }

            expect(flattened(datapoint)[0]).to.containSubset({
                metricName: 'n_1',
                timestamp: 0,
                metricValue: {
                    mango: 1.0
                }
            });
        });
        it('counts values beyond the first <bufferSize> values as "other"', () => {
            const m = new MetricCenterModel();
            const r: MetricCenterRecord = {
                id: 'a_1',
                metricName: 'n_1',
                sampleSize: 4,
                bufferSize: 2,
                timestamp: 0,
                metricValue: '',
                type: MetricType.PARTITIONING
            };

            r.metricValue = 'London';
            expect(m.put(r)).to.eql([]);

            r.metricValue = 'Cambridge';
            expect(m.put(r)).to.eql([]);

            r.metricValue = 'Oxford';
            expect(m.put(r)).to.eql([]);

            r.metricValue = 'London';
            expect(m.put(r)).to.eql([{
                absolute: 4,
                metricName: 'n_1',
                timestamp: 0,
                metricValue: {
                    "London": 0.5,
                    "Cambridge": 0.25,
                    "others": 0.25
                }
            }]);
        });

    });
});

function flattened(arr) {
    return [].concat(...arr);
}
