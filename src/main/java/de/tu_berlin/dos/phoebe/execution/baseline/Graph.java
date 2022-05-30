package de.tu_berlin.dos.phoebe.execution.baseline;

import com.google.gson.JsonObject;
import de.tu_berlin.dos.phoebe.execution.Job;
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
            context.clientsManager.createNamespace();
            // deploy to k8s using helm
            context.k8s.find("HELM").forEach(context.clientsManager::deployHelm);
            // deploy to k8s using yaml
            context.k8s.find("YAML").forEach(context.clientsManager::deployYaml);
            // waiting for load generators to come online
            new CountDownLatch(1).await(30, TimeUnit.SECONDS);
            // upload job jar
            LOG.info("Uploading Jar...");
            Job.jarId = context.clientsManager.uploadJar();
            LOG.info("Jar uploaded with id: " + Job.jarId);
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

                    int generatorsCount = context.clientsManager.generatorsClients.size();
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
            context.clientsManager.initGenerators().startGenerators(body);

            // start the target job
            for (Job job : context.jobs) {

                job.setJobId(context.clientsManager.startJob(Job.jarId, job.getProgramArgsList()));
                job.addTs(context.clientsManager.getLatestTs(job.getJobId()));
                LOG.info("Started job with configuration: " + job.getProgramArgsList().toString());
            }
            return EXECUTE;
        }
    },
    EXECUTE {

        public Graph runStage(Context context) throws Exception {

            // wait for the length of the experiment runtime
            LOG.info(Arrays.toString(context.failures.toArray()));
            context.eventTimer.register(new Listener(context.failures, () -> {

                for (Job job : context.jobs) {

                    String taskManager = context.clientsManager.getTaskManagers(job.getJobId()).iterator().next();
                    LOG.info("Injecting delay into taskManager: " + taskManager);
                    context.clientsManager.injectDelay(taskManager);
                    new CountDownLatch(5).await(1000, TimeUnit.MILLISECONDS);
                }
            }));
            context.eventTimer.start(context.expLen);

            // gather metrics and stop jobs
            for (Job job : context.jobs) {

                long firstTs = job.getFirstTs();
                long currTs = context.clientsManager.getLatestTs(job.getJobId());
                job.addTs(currTs);

                LOG.info("Gathering metrics and stopping job with configuration: " + job);
                String dirPath = String.format("%s/%s", context.dataPath, context.expId);
                Map<String, TimeSeries> metrics = new HashMap<>();
                metrics.put(String.format("%s_%s_workRate.out", dirPath, job.name), context.clientsManager.getWorkload(context.executor, firstTs, currTs));
                metrics.put(String.format("%s_%s_thrRate.out", dirPath, job.name), context.clientsManager.getThr(job.name, firstTs, currTs));
                metrics.put(String.format("%s_%s_latency.out", dirPath, job.name), context.clientsManager.getLat(job.name, firstTs, currTs));
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

                context.clientsManager.stopJob(job.getJobId());
                job.isActive(false);
            }
            // stop load generators
            context.clientsManager.initGenerators();
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
