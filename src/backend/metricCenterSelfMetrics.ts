import {MetricFactory} from './MetricFactory'

const lambdaFactory = new MetricFactory(1);

// Instead of samplSize use shceduled flushing. if samplesize is present it oerrides scheduled flsuhing (should be used rarely).

export const selfMetrics = {
    lambdaBuildMetricCenter: lambdaFactory.newPartitioning({name: 'lambda_build.metricCenter', sampleSize: 100, bufferSize: 5}),
    lambdaInvocationMetricCenter: lambdaFactory.newRate({name: 'lambda_invocation.metricCenter', sampleSize: 50}),
    metricCenterNumScheduledDatapoints: lambdaFactory.newRate({name: 'metricCenter.num_scheduled_datapoints', sampleSize: 1}),
    metricCenterNumRecords: lambdaFactory.newPercentile({name: 'metricCenter.num_records', sampleSize: 100}),
    metricCenterOldestRecordAge: lambdaFactory.newPercentile({name: 'metricCenter.oldest_record_age', sampleSize: 100}),
    metricCenterDups: lambdaFactory.newRate({name: 'metricCenter.dups', sampleSize: 100}),
    metricCenterDedupBufferSize: lambdaFactory.newGauge({name: 'metricCenter.dedup_buffer_size', sampleSize: 100}),
    metricCenterLambdaAge: lambdaFactory.newGauge({name: 'metricCenter.lambda_age', sampleSize: 1000}),
    factory: lambdaFactory
};
