import * as AWS from 'aws-sdk';
import { InvocationRequest } from 'aws-sdk/clients/lambda';

export async function sendErrorToSink(err, mapping, context, details) {
    try {
        const lambda = new AWS.Lambda({region: mapping.errorSinkFunction.region});
        const req = {
            message: err.message,
            stack: err.stack,
            origin: context.functionName,
            details: Object.assign({originRequestId: context.AWSrequestID}, details),
            remainingTimeInMillis: context.getRemainingTimeInMillis()
        }

        const lambdaParams = {
            FunctionName: mapping.errorSinkFunction.name,
            InvocationType: 'Event', 
            Payload: JSON.stringify(req),
        };

        await lambda.invoke(lambdaParams).promise();
        return Promise.resolve();
    } catch (e) {
        // Intentionally absorb
    }
}


export async function sendLogToLogSampler(key: string, data: any, mapping) {
    try {
        const lambda = new AWS.Lambda({region: mapping.logSamplerFunction.region});
        const req = {
            logSamplerStoreRequest: {                
                key,
                data
            }
        };

        const lambdaParams: InvocationRequest = {
            FunctionName: mapping.logSamplerFunction.name,
            InvocationType: 'Event', 
            Payload: JSON.stringify(req),
        };

        await lambda.invoke(lambdaParams).promise();
        return Promise.resolve();
    } catch (e) {
        // Intentionally absorb
    }
}

