export interface Options {
    polyvalue?: string[]
    datapointIntervalMillis?: number
}

export interface Timeframe {
    fromTimestamp: number
    toTimestamp: number
}

export enum Formula {
    FRACTION = "FRACTION",
    PARTS = "PARTS",
    FRACTION_COMPLEMENT = "FRACTION_COMPLEMENT"
}

export enum TimedStreamMapper {
    MIN_MAX = "MIN_MAX",
    AVERAGE = "AVERAGE"
}


export interface Per {
    metricName: string
    formula?: string,
    options?: Options
}

export interface Query {
    metricName: string
    per?: Per
    identifier?: string
    timeframe?: Timeframe
    options?: Options
}


