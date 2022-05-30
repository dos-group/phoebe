package de.tu_berlin.dos.phoebe.execution.reactive;

import com.google.gson.JsonObject;
import de.tu_berlin.dos.phoebe.structures.SequenceFSM;
import de.tu_berlin.dos.phoebe.structures.TimeSeries;
import de.tu_berlin.dos.phoebe.utils.EventTimer.Listener;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public enum Graph implements SequenceFSM<Context, Graph> {

    START {

        public Graph runStage(Context context) {

            return DEPLOY;
        }
    },
    DEPLOY {

        public Graph runStage(Context context) throws Exception {

            // re-initialize namespace
            context.cm.createNamespace();
            // deploy to k8s using helm
            context.k8s.find("HELM").forEach(context.cm::deployHelm);
            // deploy to k8s using yaml
            context.k8s.find("YAML").forEach(context.cm::deployYaml);
            // waiting for load generators to come online
            new CountDownLatch(1).await(30, TimeUnit.SECONDS);
            return INITIALIZE;
        }
    },
    INITIALIZE {

        public Graph runStage(Context context) throws Exception {

            // setup generators
            JsonObject body = new JsonObject();
            body.addProperty("brokerList", context.brokerList);
            body.addProperty("topic", context.tarConsTopic);
            body.addProperty("generatorType", context.generatorType);
            body.addProperty("limiterType", context.limiterType);
            body.addProperty("limiterMaxNoise", context.limiterMaxNoise);
            // based on supplied generator type, set properties
            switch (context.limiterType) {

                case "SINE" -> {

                    int generatorsCount = context.cm.generatorsClients.size();
                    body.addProperty("amplitude", context.amplitude / generatorsCount);
                    body.addProperty("verticalPhase", context.verticalPhase / generatorsCount);
                    body.addProperty("period", context.period);
                }
                case "DATASET" -> {

                    body.addProperty("fileName", context.fileName);
                }
                default -> throw new IllegalStateException("Unknown generator type: " + context.generatorType);
            }
            LOG.info("Start workload generation with configuration: " + body);
            context.cm.initGenerators().startGenerators(body);

            return EXECUTE;
        }
    },
    EXECUTE {

        public Graph runStage(Context context) throws Exception {

            // wait for the length of the experiment runtime
            LOG.info(Arrays.toString(context.failures.toArray()));
            context.et.register(new Listener(context.failures, () -> {

                List<String> pods = context.cm.getPodNames("component", "taskmanager");
                if (pods.size() > 0) {

                    LOG.info("Injecting delay into taskManager: " + pods.get(0));
                    //context.clientsManager.injectDelay(pods.get(0));
                    context.cm.injectFailure(pods.get(0));
                }
                else LOG.info("No task managers to inject failures");
            }));
            context.et.start(context.expLen);
            // gather metrics and stop jobs
            long firstTs = context.job.getFirstTs();
            long currTs = context.cm.getLatestTs(context.job.getJobId());
            context.job.addTs(currTs);

            LOG.info("Gathering metrics and stopping job with configuration: " + context.job);
            String dirPath = String.format("%s/%s", context.dataPath, context.expId);
            Map<String, TimeSeries> metrics = new HashMap<>();
            metrics.put(String.format("%s_%s_workRate.out", dirPath, context.job.name), context.cm.getWorkload(context.executor, firstTs, currTs));
            metrics.put(String.format("%s_%s_thrRate.out", dirPath, context.job.name), context.cm.getThr(context.job.name, firstTs, currTs));
            metrics.put(String.format("%s_%s_latency.out", dirPath, context.job.name), context.cm.getLat(context.job.name, firstTs, currTs));
            metrics.put(String.format("%s_%s_numWorkers.out", dirPath, "reactive"), context.cm.getNumWorkers(firstTs, currTs));
            // write metrics to file
            CountDownLatch latch = new CountDownLatch(8);
            metrics.forEach((name, timeSeries) -> {

                context.executor.submit(() -> {

                    try { TimeSeries.toCSV(name, timeSeries, "timestamp|value", "|"); }
                    catch (IOException e) { e.printStackTrace(); }
                    finally { latch.countDown(); }
                });
            });
            latch.await();
            // stop job
            context.cm.stopJob(context.job.getJobId());
            context.job.isActive(false);
            // stop load generators
            context.cm.initGenerators();

            return STOP;
        }
    },
    STOP {

        public Graph runStage(Context context) throws Exception {

            context.close();
            return this;
        }
    };

    public static void start(String propsFile) throws Exception {

        Graph.START.run(Graph.class, Context.create(propsFile));
    }
}
