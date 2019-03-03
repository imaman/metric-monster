import {sendErrorToSink, sendLogToLogSampler} from './errorSink'
import * as BigbandCore from 'bigband-core'

export abstract class AbstractController<T, R> extends BigbandCore.AbstractController<T, R> {
    protected async onError(e: Error) {
        await sendErrorToSink(e, this.mapping, this.context, {});
    }

    protected sendLogToSampler(key: string, data: any) {
        sendLogToLogSampler(key, data, this.mapping);
    }
}


