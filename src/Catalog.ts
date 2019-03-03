import {Metric, MetricValue, MetricFactory} from './MetricFactory';
import {selfMetrics} from './metricCenterSelfMetrics'

export class Catalog {
    private readonly stringMetricsByName: Map<string, Metric<string>>;
    private readonly numericMetricsByName: Map<string, Metric<number>>;
    private readonly factories = new Set<MetricFactory>();

    private static buildMetricMap<T extends MetricValue>(metrics: Metric<T>[]): Map<string, Metric<T>> {
        const ret = new Map<string, Metric<T>>();
        metrics.forEach(curr => {
            ret.set(curr.name(), curr);
        });
        return ret;
    }

    constructor(stringMetrics: Metric<string>[], numericMetrics: Metric<number>[]) {        
        this.addFactories(stringMetrics);
        this.addFactories(numericMetrics);
        this.factories.add(selfMetrics.factory);

        this.stringMetricsByName = Catalog.buildMetricMap(stringMetrics);
        this.numericMetricsByName = Catalog.buildMetricMap(numericMetrics);
    }

    get allMetrics() {
        const ret: Metric<MetricValue>[] = [];
        for (const curr of this.allFactories) {
            ret.push(...curr.metrics());
        }
        return ret;
    }

    get allFactories() {
        return [...this.factories.values()];
    }

    private addFactories(metrics: Metric<MetricValue>[]) {
        for (const curr of metrics) {
            this.factories.add(curr.factory)
        }
    }

    getStringMetric(name: string): Metric<string>|undefined {
        return this.stringMetricsByName.get(name);
    }

    getNumericMetric(name: string): Metric<number>|undefined {
        return this.numericMetricsByName.get(name);
    }
}
