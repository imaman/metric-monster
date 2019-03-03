import * as chai from 'chai';
import chaiSubset = require('chai-subset');

chai.use(chaiSubset);
const {expect} = chai;

import 'mocha';
import {AbstractGetDataPointsController, rateMapper} from '../src/AbstractGetDataPointsController'
import { Formula } from '../src/DataTypes'
import { QueryInput } from 'aws-sdk/clients/dynamodb';
import { MetricType } from '../src/MetricFactory';
import { Mapper } from '../src/TimedStream';


class CustomGetDataPointsController extends AbstractGetDataPointsController {
    constructor(mapping) {
        super(mapping, 'dont-care');
    }

    readonly requests: QueryInput[] = [];
    readonly responses: any[] = [];

    run(input) {
        return this.runLambda(input, {});
    }

    setMetricType(meticName: string, type: MetricType) {
        this.typeByMetricName.set(meticName, type);
        return this;
    }

    setMapper(metricName: string, mapper: Mapper) {
        this.mapperByMetricName.set(metricName, mapper);
        return this;
    }

    addResponse(resp) {
        this.responses.push(resp);
    }

    async fetchData(queryReq: QueryInput) {
        this.requests.push(queryReq);
        return this.responses.shift();
    }
}
describe('GetDataPointsController', () => {
    describe('basics', () => {
        it('sends a DynamoDB query', async () => {
            const c = new CustomGetDataPointsController({datapointsTable: {
                region: 'R_1',
                name: 'N_1'
            }});
            c.addResponse([]);
            await c.run({
                timeframe: {
                    fromTimestamp: 100,
                    toTimestamp: 200
                },
                queries: [
                    { metricName: 'M_1', options: {polyvalue: []} }
                ]
            });
        
            expect(c.requests).to.eql([
                {
                    "ExpressionAttributeValues": {
                        ":from": 100,
                        ":n": "M_1",
                        ":to": 200
                    },
                    "KeyConditionExpression": "n = :n and (t between :from and :to)",
                    "TableName": "N_1"
                }
            ]);
        });

        it("transforms the query's response", async () => {
            const c = new CustomGetDataPointsController({datapointsTable: {
                region: 'R_1',
                name: 'N_1'
            }});

            c.addResponse([
                {
                    "n": "_",
                    "v": 10,
                    "t": 101
                },
                {
                    "n": "_",
                    "v": 20,
                    "t": 102
                },
                {
                    "n": "_",
                    "v": 30,
                    "t": 103
                },
            ]);     
            const out = await c.run({
                timeframe: {
                    fromTimestamp: 100,
                    toTimestamp: 200
                },
                queries: [
                    { metricName: 'M_1', options: {} }
                ]
            });
            
            expect(out).to.eql({"output":[
                {
                    "query": {
                        "metricName": "M_1",
                        "options": {},
                        "timeframe": {
                            "fromTimestamp": 100,
                            "toTimestamp": 200
                        }
                    },
                    "timestamps":[101,102,103],
                    "sigma": {"DEFAULT": 0},
                    "values":{
                        "DEFAULT":[10,20,30]
                    }
                }
            ]});    
        });

        it("flattens polyvalue", async () => {
            const c = new CustomGetDataPointsController({datapointsTable: {
                region: 'R_1',
                name: 'N_1'
            }});
            c.addResponse([
                {
                    "n": "_",
                    "v": {v1: 'a1', v2: 'b1'},
                    "t": 101
                },
                {
                    "n": "_",
                    "v": {v1: 'a2', v2: 'b2'},
                    "t": 102
                },
                {
                    "n": "_",
                    "v": {v1: 'a3', v2: 'b3'},
                    "t": 103
                },
            ]);     

            const out = await c.run({
                timeframe: {
                    fromTimestamp: 100,
                    toTimestamp: 200
                },
                queries: [
                    { metricName: 'M_1', options: {polyvalue: []} }
                ]
            });
            
            expect(out).to.eql({"output":[
                {
                    "query": {
                        "metricName": "M_1",
                        "options": {
                            "polyvalue": []
                        },
                        "timeframe": {
                            "fromTimestamp": 100,
                            "toTimestamp": 200
                        }
                    },
                    "timestamps": [101, 102, 103],
                    "sigma": {"v1": 0, "v2": 0},
                    "values": {
                        "v1": ['a1', 'a2', 'a3'],
                        "v2": ['b1', 'b2', 'b3']
                    }
                }
            ]});    
        });

        it("picks only series that are mentioned in the polyvalue array", async () => {
            const c = new CustomGetDataPointsController({datapointsTable: {
                region: 'R_1',
                name: 'N_1'
            }});
            c.addResponse([
                {
                    "n": "_",
                    "v": {v1: 'a1', v2: 'b1', v3: 'c1'},
                    "t": 101
                },
                {
                    "n": "_",
                    "v": {v1: 'a2', v2: 'b2', v3: 'c2'},
                    "t": 102
                },
                {
                    "n": "_",
                    "v": {v1: 'a3', v2: 'b3', v3: 'c3'},
                    "t": 103
                },
            ]);     

            const out = await c.run({
                timeframe: {
                    fromTimestamp: 100,
                    toTimestamp: 200
                },
                queries: [
                    { metricName: 'M_1', options: {polyvalue: ['v1', 'v3']} }
                ]
            });
            
            expect(out).to.eql({"output":[
                {
                    "query": {
                        "metricName": "M_1",
                        "options": {
                            "polyvalue": ["v1", "v3"]
                        },
                        "timeframe": {
                            "fromTimestamp": 100,
                            "toTimestamp": 200
                        }
                    },
                    "timestamps": [101, 102, 103],
                    "sigma": {"v1": 0, "v3": 0},
                    "values": {
                        "v1": ['a1', 'a2', 'a3'],
                        "v3": ['c1', 'c2', 'c3']
                    }
                }
            ]});    
        });

        it("supports multiple queries", async () => {
            const c = new CustomGetDataPointsController({datapointsTable: {
                region: 'R_1',
                name: 'N_1'
            }});
            c.addResponse([
                { "n": "M_1", "v": 63, "t": 30  },
                { "n": "M_1", "v": 64, "t": 40  },
                { "n": "M_1", "v": 65, "t": 50  },
            ]);
            c.addResponse([
                { "n": "M_2", "v": 73, "t": 31  },
                { "n": "M_2", "v": 74, "t": 41  },
                { "n": "M_2", "v": 75, "t": 51  },
            ]);

            const out = await c.run({
                timeframe: { fromTimestamp: 20, toTimestamp: 55 },
                queries: [
                    { metricName: 'M_1'},
                    { metricName: 'M_2'}
                ]
            });
            
            expect(out).to.eql({"output":[
                {
                    "query": {
                        "metricName": "M_1",
                        "timeframe": { "fromTimestamp": 20, "toTimestamp": 55 }
                    },
                    "timestamps": [30, 40, 50],
                    "sigma": {"DEFAULT": 0},
                    "values": {
                        "DEFAULT": [63, 64, 65]
                    }
                },
                {
                    "query": {
                        "metricName": "M_2",
                        "timeframe": { "fromTimestamp": 20, "toTimestamp": 55 }
                    },
                    "timestamps": [31, 41, 51],
                    "sigma": {"DEFAULT": 0},
                    "values": {
                        "DEFAULT": [73, 74, 75]
                    }
                }
            ]});    
        });

        it("retains a query identifier", async () => {
            const c = new CustomGetDataPointsController({datapointsTable: {
                region: 'R_1',
                name: 'N_1'
            }});
            c.addResponse(createResponseV("M_1", [30, 63], [40, 64], [50, 65]));
            c.addResponse(createResponseV("M_2", [31, 73], [41, 74], [51, 75]));

            const out = await c.run({
                timeframe: { fromTimestamp: 20, toTimestamp: 55 },
                queries: [
                    { identifier: 'I_600', metricName: 'M_1'},
                    { identifier: 'I_700', metricName: 'M_2'}
                ]
            });
            
            expect(out).to.containSubset({"output":[
                {
                    "query": {
                        "metricName": "M_1",
                        "identifier": "I_600",
                        "timeframe": { "fromTimestamp": 20, "toTimestamp": 55 }
                    },
                    "timestamps": [30, 40, 50],
                    "values": {
                        "DEFAULT": [63, 64, 65]
                    }
                },
                {
                    "query": {
                        "metricName": "M_2",
                        "identifier": "I_700",
                        "timeframe": { "fromTimestamp": 20, "toTimestamp": 55 }
                    },
                    "timestamps": [31, 41, 51],
                    "values": {
                        "DEFAULT": [73, 74, 75]
                    }
                }
            ]});    
        });
    });
    describe('sigma', () => {
        it('calculates the integral', async () => {
            const c = new CustomGetDataPointsController({datapointsTable: { region: 'R_1', name: 'N_1' }})
                .setMetricType('M_A', MetricType.RATE);

            c.addResponse(createResponseA("M_A", [100000, 120], [110000, 500], [120000, 270], [130000, 240]));

            const out = await c.run({
                timeframe: {fromTimestamp: 100000, toTimestamp: 140000},                
                queries: [{ metricName: 'M_A', options: {datapointIntervalMillis: 10000} }]
            });

            expect(out).to.containSubset({output: [{sigma: {DEFAULT: 1130}}]});
        });
        it('calculates the integral from the absolute (.a) value', async () => {
            const c = new CustomGetDataPointsController({datapointsTable: { region: 'R_1', name: 'N_1' }})
                .setMetricType('M_A', MetricType.RATE)
                .setMapper('M_A', rateMapper);

            c.addResponse(createResponseA("M_A", [100010, 20], [100020, 73]));

            const out = await c.run({
                timeframe: {fromTimestamp: 100000, toTimestamp: 102000},                
                queries: [{ metricName: 'M_A', options: {datapointIntervalMillis: 1200} }]
            });

            expect(out).to.containSubset({output: [{sigma: {DEFAULT: 93}}]});
        });
    });
    describe('ratio', () => {
        it("with Formula.FRACTION it divides the corresponding values from the two streams", async () => {
            const c = new CustomGetDataPointsController({datapointsTable: {
                region: 'R_1',
                name: 'N_1'
            }});
            c.addResponse([
                {
                    "n": "M_A",
                    "v": 10,
                    "t": 101
                },
                {
                    "n": "M_A",
                    "v": 20,
                    "t": 102
                },
                {
                    "n": "M_A",
                    "v": 16,
                    "t": 103
                },
            ]);     
            c.addResponse([
                {
                    "n": "M_B",
                    "v": 2,
                    "t": 101
                },
                {
                    "n": "M_B",
                    "v": 5,
                    "t": 102
                },
                {
                    "n": "M_B",
                    "v": 8,
                    "t": 103
                },
            ]);     

            const out = await c.run({
                timeframe: {
                    fromTimestamp: 100,
                    toTimestamp: 200
                },
                queries: [{ metricName: 'M_A', per: {metricName: 'M_B', formula: Formula.FRACTION} }]
            });
            
            expect(out).to.eql({"output":[
                {
                    "query": {
                        "metricName": "M_A",
                        "per": {metricName: "M_B", formula: Formula.FRACTION},
                        "timeframe": {
                            "fromTimestamp": 100,
                            "toTimestamp": 200
                        }
                    },
                    "timestamps": [101, 102, 103],
                    "sigma": {"DEFAULT": 0},
                    "values": {
                        "DEFAULT": [5, 4, 2],
                    }
                }
            ]});    
        });

        it("with FRACTION_COMPLEMENT it takes the 1.0 complement of the fraction", async () => {
            const c = new CustomGetDataPointsController({datapointsTable: {
                region: 'R_1',
                name: 'N_1'
            }});
            c.addResponse(createResponseV('M_A', [10, 120], [15, 240], [20, 360]))
            c.addResponse(createResponseV('M_B', [10, 200], [15, 300], [20, 400]))
            const out = await c.run({
                timeframe: {fromTimestamp: 5, toTimestamp: 25},
                queries: [{ metricName: 'M_A', per: {metricName: 'M_B', formula: Formula.FRACTION_COMPLEMENT} }]
            });
            
            expect(out).to.eql({"output":[
                {
                    "query": {
                        "metricName": "M_A",
                        "per": {metricName: "M_B", formula: Formula.FRACTION_COMPLEMENT},
                        "timeframe": {fromTimestamp: 5, toTimestamp: 25}
                    },
                    "timestamps": [10, 15, 20],
                    "sigma": {"DEFAULT": 0},
                    "values": {
                        "DEFAULT": [0.4, 0.2, 0.1],
                    }
                }
            ]});    
        });

        it("with Formula.PARTS the whole is the sum of the two values", async () => {
            const c = new CustomGetDataPointsController({datapointsTable: {
                region: 'R_1',
                name: 'N_1'
            }});
            c.addResponse([
                {
                    "n": "M_A",
                    "v": 8,
                    "t": 101
                },
                {
                    "n": "M_A",
                    "v": 20,
                    "t": 102
                },
                {
                    "n": "M_A",
                    "v": 12,
                    "t": 103
                },
            ]);     
            c.addResponse([
                {
                    "n": "M_B",
                    "v": 2,
                    "t": 101
                },
                {
                    "n": "M_B",
                    "v": 60,
                    "t": 102
                },
                {
                    "n": "M_B",
                    "v": 8,
                    "t": 103
                },
            ]);     

            const out = await c.run({
                timeframe: {
                    fromTimestamp: 100,
                    toTimestamp: 200
                },
                queries: [{ metricName: 'M_A', per: {metricName: 'M_B', formula: Formula.PARTS} }]
            });
            
            expect(out).to.eql({"output":[
                {
                    "query": {
                        "metricName": "M_A",
                        "per": {metricName: "M_B", formula: Formula.PARTS},
                        "timeframe": {
                            "fromTimestamp": 100,
                            "toTimestamp": 200
                        }
                    },
                    "timestamps": [101, 102, 103],
                    "sigma": {"DEFAULT": 0},
                    "values": {
                        "DEFAULT": [0.8, 0.25, 0.6],
                    }
                }
            ]});    
        });
        it("predicts the value of the divider based on the two surrounding points", async () => {
            const c = new CustomGetDataPointsController({datapointsTable: {
                region: 'R_1',
                name: 'N_1'
            }});
            c.addResponse([
                {
                    "n": "M_A",
                    "v": 48,
                    "t": 120
                }
            ]);     
            c.addResponse([
                {
                    "n": "M_B",
                    "v": 10,
                    "t": 110
                },
                {
                    "n": "M_B",
                    "v": 14,
                    "t": 130
                },
            ]);     

            const out = await c.run({
                timeframe: {
                    fromTimestamp: 100,
                    toTimestamp: 200
                },
                queries: [{ metricName: 'M_A', per: {metricName: 'M_B', formula: Formula.FRACTION} }]
            });
            
            expect(out).to.eql({"output":[
                {
                    "query": {
                        "metricName": "M_A",
                        "per": {metricName: "M_B", formula: Formula.FRACTION},
                        "timeframe": {
                            "fromTimestamp": 100,
                            "toTimestamp": 200
                        }
                    },
                    "timestamps": [120],
                    "sigma": {"DEFAULT": 0},
                    "values": {
                        "DEFAULT": [4],
                    }
                }
            ]});    
        });
        it("uses linear interpolation for predicting the value of the divider", async () => {
            const c = new CustomGetDataPointsController({datapointsTable: {
                region: 'R_1',
                name: 'N_1'
            }});
            c.addResponse([
                {
                    "n": "M_A",
                    "v": 60,
                    "t": 120
                }
            ]);     
            c.addResponse([
                {
                    "n": "M_B",
                    "v": 6,
                    "t": 118
                },
                {
                    "n": "M_B",
                    "v": 36,
                    "t": 128
                },
            ]);     

            const out = await c.run({
                timeframe: {
                    fromTimestamp: 100,
                    toTimestamp: 200
                },
                queries: [{ metricName: 'M_A', per: {metricName: 'M_B', formula: Formula.FRACTION} }]
            });
            
            expect(out).to.eql({"output":[
                {
                    "query": {
                        "metricName": "M_A",
                        "per": {metricName: "M_B", formula: Formula.FRACTION},
                        "timeframe": {
                            "fromTimestamp": 100,
                            "toTimestamp": 200
                        }
                    },
                    "timestamps": [120],
                    "sigma": {"DEFAULT": 0},
                    "values": {
                        "DEFAULT": [5],
                    }
                }
            ]}); 
        });   
        it("drops start point if the divider value cannot be predicated there", async () => {
            const c = new CustomGetDataPointsController({datapointsTable: { region: 'R_1', name: 'N_1' }});
            c.addResponse([
                {
                    "n": "M_A",
                    "v": 60,
                    "t": 100
                }
            ]);     
            c.addResponse([
                {
                    "n": "M_B",
                    "v": 6,
                    "t": 118
                },
                {
                    "n": "M_B",
                    "v": 36,
                    "t": 128
                },
            ]);     

            const out = await c.run({
                timeframe: {
                    fromTimestamp: 100,
                    toTimestamp: 200
                },
                queries: [{ metricName: 'M_A', per: {metricName: 'M_B', formula: Formula.FRACTION} }]
            });
            
            expect(out).to.eql({"output":[
                {
                    "query": {
                        "metricName": "M_A",
                        "per": {metricName: "M_B", formula: Formula.FRACTION},
                        "timeframe": {
                            "fromTimestamp": 100,
                            "toTimestamp": 200
                        }
                    },
                    "timestamps": [],
                    "sigma": {"DEFAULT": 0},
                    "values": {
                        "DEFAULT": [],
                    }
                }
            ]}); 
        });   
        it("drops end point if the divider value cannot be predicated there", async () => {
            const c = new CustomGetDataPointsController({datapointsTable: { region: 'R_1', name: 'N_1' }});
            c.addResponse(createResponseV("M_A", [150, 60]));
            c.addResponse(createResponseV("M_B", [118, 6], [128, 36]));

            const out = await c.run({
                timeframe: {
                    fromTimestamp: 100,
                    toTimestamp: 200
                },
                queries: [{ metricName: 'M_A', per: {metricName: 'M_B', formula: Formula.FRACTION} }]
            });
            
            expect(out).to.eql({"output":[
                {
                    "query": {
                        "metricName": "M_A",
                        "per": {metricName: "M_B", formula: "FRACTION"},
                        "timeframe": {
                            "fromTimestamp": 100,
                            "toTimestamp": 200
                        }
                    },
                    "timestamps": [],
                    "sigma": {"DEFAULT": 0},
                    "values": {
                        "DEFAULT": [],
                    }
                }
            ]}); 
        });   
    });
    describe('computation at serving time (datapointIntervalMillis is specified)', () => {
        describe('of RATE metrics with MIN_MAX mapper', () => {
            it('picks the high and low values', async () => {
                const c = new CustomGetDataPointsController({datapointsTable: { region: 'R_1', name: 'N_1' }})
                    .setMetricType('M_A', MetricType.RATE);

                c.addResponse(createResponseA("M_A", 
                    [100000, 15], [100200, 70], [100400, 20], [100600, 6], [100800, 9]))

                const out = await c.run({
                    timeframe: {fromTimestamp: 100000, toTimestamp: 101000},
                    queries: [{ metricName: 'M_A', options: {datapointIntervalMillis: 1000} }]
                });

                expect(out).to.eql({"output":[
                    {
                        "query": {
                            "metricName": "M_A",
                            "options": {"datapointIntervalMillis": 1000},
                            "timeframe": {
                                "fromTimestamp": 100000,
                                "toTimestamp": 101000
                            }
                        },
                        "timestamps":[100200, 100600],
                        "sigma": {"DEFAULT": 120},
                        "values":{
                            "DEFAULT":[350, 30]
                        }
                    }]});    
            });

        });
        describe('of RATE metrics with AVERAGE mapper', () => {
            it('calculates a per-second rate from the sum of the the absolute (.a) values in the interval', async () => {
                const c = new CustomGetDataPointsController({datapointsTable: { region: 'R_1', name: 'N_1' }})
                    .setMetricType('M_A', MetricType.RATE)
                    .setMapper('M_A', rateMapper);

                c.addResponse(createResponseA("M_A", [100000, 15], [103000, 9]))

                const out = await c.run({
                    timeframe: {
                        fromTimestamp: 100000,
                        toTimestamp: 104000
                    },                
                    queries: [{ metricName: 'M_A', options: {datapointIntervalMillis: 4000} }]
                });

                expect(out).to.eql({"output":[
                    {
                        "query": {
                            "metricName": "M_A",
                            "options": {"datapointIntervalMillis": 4000},
                            "timeframe": {
                                "fromTimestamp": 100000,
                                "toTimestamp": 104000
                            }
                        },
                        "timestamps":[102000],
                        "sigma": {"DEFAULT": 24},
                        "values":{
                            "DEFAULT":[6]
                        }
                    }]});    
            });

            it('returns all zero datapoints if response (for the given timeframe) is empty', async () => {
                const c = new CustomGetDataPointsController({datapointsTable: { region: 'R_1', name: 'N_1' }})
                    .setMetricType('M_A', MetricType.RATE)
                    .setMapper('M_A', rateMapper);

                c.addResponse(createResponseA("M_A"))

                const out = await c.run({
                    timeframe: {
                        fromTimestamp: 100000,
                        toTimestamp: 112000
                    },                
                    queries: [{ metricName: 'M_A', options: {datapointIntervalMillis: 4000} }]
                });

                expect(out).to.eql({"output":[
                    {
                        "query": {
                            "metricName": "M_A",
                            "options": {"datapointIntervalMillis": 4000},
                            "timeframe": {
                                "fromTimestamp": 100000,
                                "toTimestamp": 112000
                            }
                        },
                        "timestamps":[102000, 106000, 110000],
                        "sigma": {"DEFAULT": 0},
                        "values":{
                            "DEFAULT":[0, 0, 0]
                        }
                    }]});    
            });
            it('it does so separately for each interval', async () => {
                const c = new CustomGetDataPointsController({datapointsTable: { region: 'R_1', name: 'N_1' }})
                    .setMetricType('M_A', MetricType.RATE)
                    .setMapper('M_A', rateMapper);

                c.addResponse(createResponseA("M_A",
                    [100000, 15], [103000, 9], 
                    [105000, 30], [106000, 26], [107000, 12],
                    [109000, 36]))

                const out = await c.run({
                    timeframe: {
                        fromTimestamp: 100000,
                        toTimestamp: 112000
                    },                
                    queries: [{ metricName: 'M_A', options: {datapointIntervalMillis: 4000} }]
                });

                expect(out).to.eql({"output":[
                    {
                        "query": {
                            "metricName": "M_A",
                            "options": {"datapointIntervalMillis": 4000},
                            "timeframe": {
                                "fromTimestamp": 100000,
                                "toTimestamp": 112000
                            }
                        },
                        "timestamps":[102000, 106000, 110000],
                        "sigma": {"DEFAULT": 128},
                        "values":{
                            "DEFAULT":[6, 17, 9] 
                        }
                    }]});    
            });
            it('interval starts are inclusive, interval ends are exclusive', async () => {
                const c = new CustomGetDataPointsController({datapointsTable: { region: 'R_1', name: 'N_1' }})
                    .setMetricType('M_A', MetricType.RATE)
                    .setMapper('M_A', rateMapper);

                c.addResponse(createResponseA("M_A",[104000, 52]));

                const out = await c.run({
                    timeframe: {
                        fromTimestamp: 100000,
                        toTimestamp: 112000
                    },                
                    queries: [{ metricName: 'M_A', options: {datapointIntervalMillis: 4000} }]
                });

                expect(out).to.eql({"output":[
                    {
                        "query": {
                            "metricName": "M_A",
                            "options": {"datapointIntervalMillis": 4000},
                            "timeframe": {
                                "fromTimestamp": 100000,
                                "toTimestamp": 112000
                            }
                        },
                        "timestamps":[102000, 106000, 110000],
                        "sigma": {"DEFAULT": 52},
                        "values":{
                            "DEFAULT":[0, 13, 0] 
                        }
                    }]});    
            });
            it('handles timeframes which do not perfectly align with the datapoint interval', async () => {
                const c = new CustomGetDataPointsController({datapointsTable: { region: 'R_1', name: 'N_1' }})
                    .setMetricType('M_A', MetricType.RATE)
                    .setMapper('M_A', rateMapper);

                    c.addResponse(createResponseA("M_A",
                        [103000, 7], 
                        [105000, 20], [111000, 8],
                        [113000, 35]))

                const out = await c.run({
                    timeframe: {
                        fromTimestamp: 100000,
                        toTimestamp: 119000
                    },                
                    queries: [{ metricName: 'M_A', options: {datapointIntervalMillis: 7000} }]
                });

                expect(out).to.eql({"output":[
                    {
                        "query": {
                            "metricName": "M_A",
                            "options": {"datapointIntervalMillis": 7000},
                            "timeframe": {
                                "fromTimestamp": 100000,
                                "toTimestamp": 119000
                            }
                        },
                        "timestamps":[101500, 108500, 115500],
                        "sigma": {"DEFAULT": 70},
                        "values":{
                            "DEFAULT":[1, 4, 5] 
                        }
                    }]});    
            });
            it('adjusts the timestamp of the first datapoint to the beginning of the timeframe if it falls outside of it', async () => {
                const c = new CustomGetDataPointsController({datapointsTable: { region: 'R_1', name: 'N_1' }})
                    .setMetricType('M_A', MetricType.RATE)
                    .setMapper('M_A', rateMapper);

                    c.addResponse(createResponseA("M_A",
                        [103000, 7], 
                        [106000, 14]))

                const out = await c.run({
                    timeframe: {
                        fromTimestamp: 104000,
                        toTimestamp: 112000
                    },                
                    queries: [{ metricName: 'M_A', options: {datapointIntervalMillis: 7000} }]
                });

                expect(out).to.eql({"output":[
                    {
                        "query": {
                            "metricName": "M_A",
                            "options": {"datapointIntervalMillis": 7000},
                            "timeframe": {
                                "fromTimestamp": 104000,
                                "toTimestamp": 112000
                            }
                        },
                        "timestamps":[104000, 108500],
                        "sigma": {"DEFAULT": 21},
                        "values":{
                            "DEFAULT":[1, 2] 
                        }
                    }]});    
            });
            it('drops the last datapoint if its interval partially falls outside of the timeframe', async () => {
                const c = new CustomGetDataPointsController({datapointsTable: { region: 'R_1', name: 'N_1' }})
                    .setMetricType('M_A', MetricType.RATE)
                    .setMapper('M_A', rateMapper);

                    c.addResponse(createResponseA("M_A",
                        [101000, 4], 
                        [107000, 12]))

                const out = await c.run({
                    timeframe: {fromTimestamp: 100000, toTimestamp: 105000},                
                    queries: [{ metricName: 'M_A', options: {datapointIntervalMillis: 4000} }]
                });

                expect(out).to.eql({"output":[
                    {
                        "query": {
                            "metricName": "M_A",
                            "options": {"datapointIntervalMillis": 4000},
                            "timeframe": {"fromTimestamp": 100000, "toTimestamp": 105000}
                        },
                        "timestamps":[102000],
                        "sigma": {"DEFAULT": 4},
                        "values":{
                            "DEFAULT":[1] 
                        }
                    }]});    
            });        
            it('fallsback to original relative values (.v) if the absolute value (.a) is missing', async () => {
                const c = new CustomGetDataPointsController({datapointsTable: { region: 'R_1', name: 'N_1' }})
                    .setMetricType('M_A', MetricType.RATE)
                    .setMapper('M_A', rateMapper);

                    c.addResponse(createResponseV("M_A",
                        [101000, 4], 
                        [107000, 12]))

                const out = await c.run({
                    timeframe: {fromTimestamp: 100000, toTimestamp: 110000},                
                    queries: [{ metricName: 'M_A', options: {datapointIntervalMillis: 4000} }]
                });

                expect(out).to.eql({"output":[
                    {
                        "query": {
                            "metricName": "M_A",
                            "options": {"datapointIntervalMillis": 4000},
                            "timeframe": {"fromTimestamp": 100000, "toTimestamp": 110000}
                        },
                        "timestamps":[101000, 107000],
                        "sigma": {"DEFAULT": 0},
                        "values":{
                            "DEFAULT":[4, 12]
                        }
                    }]});    
            });
        });
        describe('of PARTITIONING metrics', () => {
            it('calculates a per-second rate from the sum of the the absolute (.a) values in the interval', async () => {
                const c = new CustomGetDataPointsController({datapointsTable: { region: 'R_1', name: 'N_1' }})
                    .setMetricType('M_A', MetricType.PARTITIONING);

                c.addResponse(createResponseVA("M_A", [101000, {v1: 0.3, v2: 0.7}, 30], [103000, {v1: 0.4, v2: 0.6}, 20]));

                const out = await c.run({
                    timeframe: {fromTimestamp: 100000, toTimestamp: 104000},                
                    queries: [{ metricName: 'M_A', options: {polyvalue: [], datapointIntervalMillis: 4000} }]
                });

                expect(out).to.eql({"output":[
                    {
                        "query": {
                            "metricName": "M_A",
                            "options": {"datapointIntervalMillis": 4000, "polyvalue": []},
                            "timeframe": {
                                "fromTimestamp": 100000,
                                "toTimestamp": 104000
                            }
                        },
                        "timestamps":[102000],
                        "sigma": {
                            v1: 0.34,
                            v2: 0.66
                        },
                        "values": {
                            v1: [0.34],
                            v2: [0.66]
                        }
                    }]});    
            });

            it('computes for all intervals', async () => {
                const c = new CustomGetDataPointsController({datapointsTable: { region: 'R_1', name: 'N_1' }})
                    .setMetricType('M_A', MetricType.PARTITIONING);

                c.addResponse(createResponseVA("M_A", 
                    [101000, {v1: 0.3, v2: 0.7}, 30], [103000, {v1: 0.4, v2: 0.6}, 20],
                    [105000, {v1: 0.9, v2: 0.1}, 100], [107000, {v1: 0.8, v2: 0.2}, 100],
                    [109000, {v1: 0.6, v2: 0.4}, 20], [111000, {v1: 0.5, v2: 0.5}, 80],
                    [113000, {v1: 0.25, v2: 0.75}, 400], [115000, {v1: 0.7, v2: 0.3}, 1600],
                ));

                const out = await c.run({
                    timeframe: {fromTimestamp: 100000, toTimestamp: 116000},                
                    queries: [{ metricName: 'M_A', options: {polyvalue: [], datapointIntervalMillis: 4000} }]
                });

                expect(out).to.eql({"output":[
                    {
                        "query": {
                            "metricName": "M_A",
                            "options": {"datapointIntervalMillis": 4000, "polyvalue": []},
                            "timeframe": {
                                "fromTimestamp": 100000,
                                "toTimestamp": 116000
                            }
                        },
                        "timestamps":[102000, 106000, 110000, 114000],
                        "sigma": {
                            v1: 0.62085,
                            v2: 0.37915
                        },
                        "values": {
                            v1: [0.34, 0.85, 0.52, 0.61],
                            v2: [0.66, 0.15, 0.48, 0.39]
                        }
                    }]});    
            });

            it('computes for all empty intervals', async () => {
                const c = new CustomGetDataPointsController({datapointsTable: { region: 'R_1', name: 'N_1' }})
                    .setMetricType('M_A', MetricType.PARTITIONING);

                c.addResponse(createResponseVA("M_A", 
                    [101000, {v1: 0.3, v2: 0.7}, 30], [103000, {v1: 0.4, v2: 0.6}, 20],
                    [109000, {v1: 0.6, v2: 0.4}, 20], [111000, {v1: 0.5, v2: 0.5}, 80],
                    [113000, {v1: 0.2, v2: 0.8}, 50], [115000, {v1: 0.7, v2: 0.3}, 50],
                ));

                const out = await c.run({
                    timeframe: {fromTimestamp: 96000, toTimestamp: 116000},                
                    queries: [{ metricName: 'M_A', options: {polyvalue: [], datapointIntervalMillis: 4000} }]
                });

                expect(out).to.eql({"output":[
                    {
                        "query": {
                            "metricName": "M_A",
                            "options": {"datapointIntervalMillis": 4000, "polyvalue": []},
                            "timeframe": {
                                "fromTimestamp": 96000,
                                "toTimestamp": 116000
                            }
                        },
                        "timestamps":[102000, 110000, 114000],
                        "sigma": {
                            v1: 0.456,
                            v2: 0.544
                        },
                        "values": {
                            v1: [0.34, 0.52, 0.45],
                            v2: [0.66, 0.48, 0.55]
                        }
                    }]});    
            });

            // TODO: 
            // - sigma in ratio
            // - ratio with .a
            // - ratio with recalc.
            it('treats names that are not present as zeros', async () => {
                const c = new CustomGetDataPointsController({datapointsTable: { region: 'R_1', name: 'N_1' }})
                    .setMetricType('M_A', MetricType.PARTITIONING);

                c.addResponse(createResponseVA("M_A", 
                    [101000, {v1: 0.3, v2: 0.7}, 30], [103000, {v1: 0.4, v2: 0.6}, 20],
                    [105000, {v1: 0.9, v3: 0.1}, 100], [107000, {v1: 0.8, v3: 0.2}, 100],
                ));

                const out = await c.run({
                    timeframe: {fromTimestamp: 100000, toTimestamp: 108000},                
                    queries: [{ metricName: 'M_A', options: {polyvalue: [], datapointIntervalMillis: 4000} }]
                });

                expect(out).to.eql({"output":[
                    {
                        "query": {
                            "metricName": "M_A",
                            "options": {"datapointIntervalMillis": 4000, "polyvalue": []},
                            "timeframe": {
                                "fromTimestamp": 100000,
                                "toTimestamp": 108000
                            }
                        },
                        "timestamps":[102000, 106000],
                        "sigma": {
                            v1: 0.748,
                            v2: 0.132,
                            v3: 0.12
                        },
                        "values": {
                            v1: [0.34, 0.85],
                            v2: [0.66, 0],
                            v3: [0, 0.15]
                        }
                    }]});    
            });
        });

        describe('of PERCENTILE metrics', () => {
            it('takes the median of 50%-ile', async () => {
                const c = new CustomGetDataPointsController({datapointsTable: { region: 'R_1', name: 'N_1' }})
                    .setMetricType('M_A', MetricType.PERCENTILE);

                c.addResponse(createResponseV("M_A", [101000, {p50: 30}], [103000, {p50: 200}], [103500, {p50: 70}]));

                const out = await c.run({
                    timeframe: {fromTimestamp: 100000, toTimestamp: 104000},                
                    queries: [{ metricName: 'M_A', options: {polyvalue: [], datapointIntervalMillis: 4000} }]
                });

                expect(out).to.eql({"output":[
                    {
                        "query": {
                            "metricName": "M_A",
                            "options": {"datapointIntervalMillis": 4000, "polyvalue": []},
                            "timeframe": {
                                "fromTimestamp": 100000,
                                "toTimestamp": 104000
                            }
                        },
                        "timestamps":[102000],
                        "values": {
                            p50: [70],
                        }
                    }]});    
            });
            it('takes the max of 90%-ile', async () => {
                const c = new CustomGetDataPointsController({datapointsTable: { region: 'R_1', name: 'N_1' }})
                    .setMetricType('M_A', MetricType.PERCENTILE);

                c.addResponse(createResponseV("M_A", [101000, {p90: 30}], [103000, {p90: 200}], [103500, {p90: 70}]));

                const out = await c.run({
                    timeframe: {fromTimestamp: 100000, toTimestamp: 104000},                
                    queries: [{ metricName: 'M_A', options: {polyvalue: [], datapointIntervalMillis: 4000} }]
                });

                expect(out).to.eql({"output":[
                    {
                        "query": {
                            "metricName": "M_A",
                            "options": {"datapointIntervalMillis": 4000, "polyvalue": []},
                            "timeframe": {
                                "fromTimestamp": 100000,
                                "toTimestamp": 104000
                            }
                        },
                        "timestamps":[102000],
                        "values": {
                            p90: [200],
                        }
                    }]});    
            });

            it('takes the min of 10%-ile', async () => {
                const c = new CustomGetDataPointsController({datapointsTable: { region: 'R_1', name: 'N_1' }})
                    .setMetricType('M_A', MetricType.PERCENTILE);

                c.addResponse(createResponseV("M_A", [101000, {p10: 30}], [103000, {p10: 200}], [103500, {p10: 70}]));

                const out = await c.run({
                    timeframe: {fromTimestamp: 100000, toTimestamp: 104000},                
                    queries: [{ metricName: 'M_A', options: {polyvalue: [], datapointIntervalMillis: 4000} }]
                });

                expect(out).to.eql({"output":[
                    {
                        "query": {
                            "metricName": "M_A",
                            "options": {"datapointIntervalMillis": 4000, "polyvalue": []},
                            "timeframe": {
                                "fromTimestamp": 100000,
                                "toTimestamp": 104000
                            }
                        },
                        "timestamps":[102000],
                        "values": {
                            p10: [30],
                        }
                    }]});    
            });            
            it('takes the max of max, min of min', async () => {
                const c = new CustomGetDataPointsController({datapointsTable: { region: 'R_1', name: 'N_1' }})
                    .setMetricType('M_A', MetricType.PERCENTILE);

                c.addResponse(createResponseV("M_A", [101000, {min: 400, max: 500}], [103000, {min: 200, max: 250}]));

                const out = await c.run({
                    timeframe: {fromTimestamp: 100000, toTimestamp: 104000},                
                    queries: [{ metricName: 'M_A', options: {polyvalue: [], datapointIntervalMillis: 4000} }]
                });

                expect(out).to.eql({"output":[
                    {
                        "query": {
                            "metricName": "M_A",
                            "options": {"datapointIntervalMillis": 4000, "polyvalue": []},
                            "timeframe": {
                                "fromTimestamp": 100000,
                                "toTimestamp": 104000
                            }
                        },
                        "timestamps":[102000],
                        "values": {
                            min: [200],
                            max: [500]
                        }
                    }]});    
            });            

            it('handles multiple %-ile values', async () => {
                const c = new CustomGetDataPointsController({datapointsTable: { region: 'R_1', name: 'N_1' }})
                    .setMetricType('M_A', MetricType.PERCENTILE);

                c.addResponse(createResponseV("M_A", [101000, {p4: 30, p50: 60, p86: 110}], [103000, {p4: 29, p50: 90, p86: 108}], [103500, {p4: 32, p50: 70, p86: 107}]));

                const out = await c.run({
                    timeframe: {fromTimestamp: 100000, toTimestamp: 104000},                
                    queries: [{ metricName: 'M_A', options: {polyvalue: [], datapointIntervalMillis: 4000} }]
                });

                expect(out).to.eql({"output":[
                    {
                        "query": {
                            "metricName": "M_A",
                            "options": {"datapointIntervalMillis": 4000, "polyvalue": []},
                            "timeframe": {
                                "fromTimestamp": 100000,
                                "toTimestamp": 104000
                            }
                        },
                        "timestamps":[102000],
                        "values": {
                            p4: [29],
                            p50: [70],
                            p86: [110]
                        }   
                    }]});    
            });            

            it('computes for each interval separately', async () => {
                const c = new CustomGetDataPointsController({datapointsTable: { region: 'R_1', name: 'N_1' }})
                    .setMetricType('M_A', MetricType.PERCENTILE);

                c.addResponse(createResponseV("M_A", 
                    [100001, {p4: 30, p50: 60, p86: 110}], [100002, {p4: 29, p50: 90, p86: 108}], [100003, {p4: 32, p50: 70, p86: 107}],
                    [100005, {p4: 200, p50: 220, p86: 240}], [100006, {p4: 201, p50: 230, p86: 239}], 
                    [100008, {p4: 480, p50: 580, p86: 680}], [100009, {p4: 490, p50: 590, p86: 690}], [100010, {p4: 470, p50: 570, p86: 670}]
                ));

                const out = await c.run({
                    timeframe: {fromTimestamp: 100000, toTimestamp: 100012},                
                    queries: [{ metricName: 'M_A', options: {polyvalue: [], datapointIntervalMillis: 4} }]
                });

                expect(out).to.eql({"output":[
                    {
                        "query": {
                            "metricName": "M_A",
                            "options": {"datapointIntervalMillis": 4, "polyvalue": []},
                            "timeframe": {
                                "fromTimestamp": 100000,
                                "toTimestamp": 100012
                            }
                        },
                        "timestamps":[100002, 100006, 100010],
                        "values": {
                            p4: [29, 200, 470],
                            p50: [70, 220, 580],
                            p86: [110, 240, 690]
                        }   
                    }]});    
            });            

            it('generates nothing for empty interval', async () => {
                const c = new CustomGetDataPointsController({datapointsTable: { region: 'R_1', name: 'N_1' }})
                    .setMetricType('M_A', MetricType.PERCENTILE);

                c.addResponse(createResponseV("M_A", 
                    [100, {p4: 30, p50: 60, p86: 110}], [102, {p4: 29, p50: 90, p86: 108}],
                    [108, {p4: 480, p50: 580, p86: 680}], [109, {p4: 490, p50: 590, p86: 690}]
                ));

                const out = await c.run({
                    timeframe: {fromTimestamp: 96, toTimestamp: 112},                
                    queries: [{ metricName: 'M_A', options: {polyvalue: [], datapointIntervalMillis: 4} }]
                });

                expect(out).to.eql({"output":[
                    {
                        "query": {
                            "metricName": "M_A",
                            "options": {"datapointIntervalMillis": 4, "polyvalue": []},
                            "timeframe": {
                                "fromTimestamp": 96,
                                "toTimestamp": 112
                            }
                        },
                        "timestamps":[102, 110],
                        "values": {
                            p4: [29, 480],
                            p50: [60, 580],
                            p86: [110, 690]
                        }   
                    }]});    
            });            

            it('yells if names are names of the form "p<number>"', async () => {
                const c = new CustomGetDataPointsController({datapointsTable: { region: 'R_1', name: 'N_1' }})
                    .setMetricType('M_A', MetricType.PERCENTILE);

                c.addResponse(createResponseV("M_A", [101000, {p1x: 30, p90: 40}], [103000, {pg: 200}], [103500, {z20: 70}]));

                const lambdaInput = {
                    timeframe: {fromTimestamp: 100000, toTimestamp: 104000},                
                    queries: [{ metricName: 'M_A', options: {polyvalue: [], datapointIntervalMillis: 4000} }]
                };

                const error = await await c.run(lambdaInput).then(() => {throw new Error('Should have failed')}, err => err);
                expect(error.message).to.equal('(details: metricName=M_A, type=PERCENTILE) Found bad names: p1x, pg, z20');
            });            

        });

        describe('of PERCENTILE_BOTTOM metrics', () => {
            it('recomputes', async () => {
                const c = new CustomGetDataPointsController({datapointsTable: { region: 'R_1', name: 'N_1' }})
                    .setMetricType('M_A', MetricType.PERCENTILE_BOTTOM);

                c.addResponse(createResponseV("M_A", 
                    [100001, {p4: 30, p50: 60, p86: 110}], [100002, {p4: 29, p50: 90, p86: 108}], [100003, {p4: 32, p50: 70, p86: 107}],
                    [100005, {p4: 200, p50: 220, p86: 240}], [100006, {p4: 201, p50: 230, p86: 239}], 
                    [100008, {p4: 480, p50: 580, p86: 680}], [100009, {p4: 490, p50: 590, p86: 690}], [100010, {p4: 470, p50: 570, p86: 670}]
                ));

                const out = await c.run({
                    timeframe: {fromTimestamp: 100000, toTimestamp: 100012},                
                    queries: [{ metricName: 'M_A', options: {polyvalue: [], datapointIntervalMillis: 4} }]
                });

                expect(out).to.eql({"output":[
                    {
                        "query": {
                            "metricName": "M_A",
                            "options": {"datapointIntervalMillis": 4, "polyvalue": []},
                            "timeframe": {
                                "fromTimestamp": 100000,
                                "toTimestamp": 100012
                            }
                        },
                        "timestamps":[100002, 100006, 100010],
                        "values": {
                            p4: [29, 200, 470],
                            p50: [70, 220, 580],
                            p86: [110, 240, 690]
                        }   
                    }]});    
            });            

        });


        describe('of GAUGE metrics', () => {
            it('picks a value clostest to the mid point of the interval', async () => {
                const c = new CustomGetDataPointsController({datapointsTable: { region: 'R_1', name: 'N_1' }})
                    .setMetricType('M_A', MetricType.GAUGE);

                c.addResponse(createResponseV("M_A", 
                    [101000, 30], [102900, 25], [103000, 20]));

                const out = await c.run({
                    timeframe: {fromTimestamp: 100000, toTimestamp: 104000},                
                    queries: [{ metricName: 'M_A', options: {polyvalue: [], datapointIntervalMillis: 4000} }]
                });

                expect(out).to.eql({"output":[
                    {
                        "query": {
                            "metricName": "M_A",
                            "options": {"datapointIntervalMillis": 4000, "polyvalue": []},
                            "timeframe": {
                                "fromTimestamp": 100000,
                                "toTimestamp": 104000
                            }
                        },
                        "timestamps":[102900],
                        "values": {
                            DEFAULT: [25]
                        }
                    }]});
            });

            it('picks nothing if the interval is empty', async () => {
                const c = new CustomGetDataPointsController({datapointsTable: { region: 'R_1', name: 'N_1' }})
                    .setMetricType('M_A', MetricType.GAUGE);

                c.addResponse(createResponseV("M_A", [107000, 300], [107001, 301]));

                const out = await c.run({
                    timeframe: {fromTimestamp: 100000, toTimestamp: 108000},                
                    queries: [{ metricName: 'M_A', options: {polyvalue: [], datapointIntervalMillis: 4000} }]
                });

                expect(out).to.eql({"output":[
                    {
                        "query": {
                            "metricName": "M_A",
                            "options": {"datapointIntervalMillis": 4000, "polyvalue": []},
                            "timeframe": {
                                "fromTimestamp": 100000,
                                "toTimestamp": 108000
                            }
                        },
                        "timestamps":[107000],
                        "values": {
                            DEFAULT: [300]
                        }
                    }]});
            });

        });
    });
});

function createResponseVA(metricName, ...triplets) {
    const index = triplets.findIndex(p => p.length !== 3);
    if (index >= 0) {
        throw new Error(`Oops. Found a triplet (at index ${index}) which is not a triplet (has ${triplets[index].length} members)`);
    }

    return triplets.map(curr => ({n: metricName, t: curr[0], v: curr[1], a: curr[2]}));
}

function createResponseA(metricName, ...pairs) {
    return createResponse("a", metricName, pairs);
}
function createResponseV(metricName, ...pairs) {
    return createResponse("v", metricName, pairs);
}

function createResponse(fieldName, metricName, pairs) {
    const index = pairs.findIndex(p => p.length !== 2);
    if (index >= 0) {
        throw new Error(`Oops. Found a pair (at index ${index}) which is not a pair (has ${pairs[index].length} members)`);
    }

    return pairs.map(curr => ({n: metricName, t: curr[0], [fieldName]: curr[1]}));
}

