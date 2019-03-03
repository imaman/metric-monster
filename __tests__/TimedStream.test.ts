import * as chai from 'chai';
import chaiSubset = require('chai-subset');

chai.use(chaiSubset);
const {expect} = chai;

import 'mocha';
import { TimedStream, Mapper } from '../src/TimedStream';
import { MetricType } from '../src/MetricFactory'
import { TimedRecord } from '../src/TimedRecord';
import { Polyvalue } from '../src/Polyvalue';

const NO_OPTIONS = {}

function timeframe(f, t) {
    return {fromTimestamp: f, toTimestamp: t};
}

describe('TimedStream', () => {
    describe('translation to POJO', () => {
        it('is empty if the input is empty', () => {
            const ts = TimedStream.parse([], NO_OPTIONS, timeframe(0, 100), MetricType.RATE);
            expect(ts.toPojo()).to.eql({sigma: {DEFAULT: 0}, timestamps: [], values: {DEFAULT: []}});
        })
        it('translates a single-record input into pojo', () => {
            const ts = TimedStream.parse([{t: 10, v: 30}], NO_OPTIONS, timeframe(0, 100), MetricType.RATE);
            expect(ts.toPojo()).to.eql({sigma: {DEFAULT: 0}, timestamps: [10], values: {DEFAULT: [30]}});
        })
        it('translates translates multi-record input into pojo', () => {
            const ts = TimedStream.parse([{t: 10, v: 30}, {t: 20, v: 15}, {t:90, v: 8}], 
                NO_OPTIONS, timeframe(0, 100), MetricType.RATE);
            expect(ts.toPojo()).to.eql({sigma: {DEFAULT: 0}, timestamps: [10, 20, 90], values: {DEFAULT: [30, 15, 8]}});
        })
        it('handles zero values', () => {
            const ts = TimedStream.parse([{t: 10, v: 0, a: 0}], NO_OPTIONS, timeframe(0, 100), MetricType.RATE);
            expect(ts.toPojo()).to.eql({sigma: {DEFAULT: 0}, timestamps: [10], values: {DEFAULT: [0]}});
        })
    });

    describe('interval mapping', () => {
        it('handles zero values', () => {
            const ts = TimedStream.parse([{t: 4, v: 0, a: 0}], {datapointIntervalMillis: 100}, timeframe(0, 100), MetricType.RATE);

            const g: Mapper = (_names, _records, timestamp: number, _intervalInMillis) => [new TimedRecord(timestamp, Polyvalue.parse(99), Polyvalue.parse(100))];
            const mapped = ts.mapIntervals(g);
            expect(mapped.toPojo()).to.eql({sigma: {DEFAULT: 100}, timestamps: [50], values: {DEFAULT: [99]}});
        });
    });
});
