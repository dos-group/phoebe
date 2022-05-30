package de.tu_berlin.dos.phoebe.execution.twres;

import com.google.gson.JsonObject;
import de.tu_berlin.dos.phoebe.execution.Job;
import de.tu_berlin.dos.phoebe.managers.DataManager.Profile;
import de.tu_berlin.dos.phoebe.structures.SequenceFSM;
import de.tu_berlin.dos.phoebe.structures.TimeSeries;
import de.tu_berlin.dos.phoebe.utils.EventTimer.Listener;

import java.io.IOException;
import java.util.*;
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
            // upload flink jar
            LOG.info("Uploading JAR...");
            Job.jarId = context.cm.uploadJar();
            LOG.info("Jar uploaded with id: " + Job.jarId);
            return MODEL;
        }
    },
    MODEL {

        @Override
        public Graph runStage(Context ctx) throws Exception {

            CountDownLatch latch = new CountDownLatch(1);

            List<Integer> scaleOuts = new ArrayList<>();
            List<Double> avgThrRates = new ArrayList<>();
            List<Profile> profiles = ctx.dm.getProfiles(ctx.expId, ctx.genType);
            for (Profile profile : profiles) {

                if (profile.isBckPres) {

                    scaleOuts.add(profile.scaleOut);
                    avgThrRates.add(profile.avgThr);
                }
            }
            String twresTaskHash = ctx.cm.twresTrain(scaleOuts, avgThrRates, ctx.genType);
            ctx.executor.submit(() -> {
                try {
                    boolean isRunning = true;
                    while (isRunning) {

                        isRunning = ctx.cm.analyticsClient.commonTasks(twresTaskHash);
                        LOG.info("Training model, is still running: " + isRunning);
                        Thread.sleep(1000);
                    }
                    latch.countDown();
                }
                catch (Exception e) { throw new IllegalStateException(e.getMessage()); }
            });
            // wait until model is trained
            latch.await();
            return INITIALIZE;
        }
    },
    INITIALIZE {

        public Graph runStage(Context context) throws Exception {

            // setup generators
            JsonObject body = new JsonObject();
            body.addProperty("brokerList", context.brokerList);
            body.addProperty("topic", context.tarConsTopic);
            body.addProperty("generatorType", context.genType);
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
                default -> throw new IllegalStateException("Unknown generator type: " + context.genType);
            }
            LOG.info("Start workload generation with configuration: " + body);
            context.cm.initGenerators().startGenerators(body);

            // start the target job
            context.job.setJobId(context.cm.startJob(Job.jarId, context.job.getProgramArgsList()));
            context.job.addTs(context.cm.getLatestTs(context.job.getJobId()));
            LOG.info("Started job with configuration: " + context.job.getProgramArgsList().toString());
            return OPTIMIZE;
        }
    },
    OPTIMIZE {

        public Graph runStage(Context ctx) throws Exception {

            // log initial scaleout and timestamp
            LOG.info(String.format("%d, %d", ctx.job.getScaleOut(), ctx.cm.getLatestTs(ctx.job.getJobId())));
            // execute optimization step in separate thread
            ctx.executor.submit(() -> {

                Job job = ctx.job;
                while (!Thread.currentThread().isInterrupted()) {

                    try {
                        while (true) {

                            new CountDownLatch(1).await(60, TimeUnit.SECONDS);
                            long now = ctx.cm.getLatestTs(job.getJobId());
                            //long uptime = ctx.cm.getUptime(job.name, now - 5, now);
                            long lastChange = now - job.getLastTs();
                            //LOG.info(String.format(
                                //"Wait till uptime (%d/%d) and evaluation interval (%d/%d) have expired",
                                //uptime, ctx.evalInt, lastChange, ctx.evalInt));
                            LOG.info(String.format("Wait till evaluation interval (%d/%d) has expired", lastChange, ctx.evalInt));
                            //if (ctx.evalInt <= lastChange && ctx.evalInt <= uptime) break;
                            if (ctx.evalInt <= lastChange) break;
                        }
                        LOG.info("Running evaluation for job: " + job);
                        // retrieve metrics for predictions
                        long currTs = ctx.cm.getLatestTs(job.getJobId());
                        int currScaleOut = ctx.cm.getScaleOut(job.getJobId());
                        TimeSeries workload = ctx.cm.getWorkload(ctx.executor, job.getLastTs(), currTs);
                        double avgLat = ctx.cm.getLat(job.name, (currTs - ctx.avgWindow), currTs).avg();
                        int bestScaleOut =
                            ctx.cm.twresPredict(
                                workload, 600, avgLat, ctx.avgLatConst, job.getScaleOut(),
                                ctx.minScaleOut, ctx.maxScaleOut, ctx.genType);
                        LOG.info(String.format("Current scaleout: %d, Predicted scaleout: %d", currScaleOut, bestScaleOut));
                        if (currScaleOut != bestScaleOut) {

                            LOG.info("Reconfiguration initiated, count: " + ctx.reconCount.incrementAndGet());
                            LOG.info(String.format("%d, %d", ctx.job.getScaleOut(), ctx.cm.getLatestTs(ctx.job.getJobId())));
                            JsonObject body = job.getProgramArgsList();
                            body.addProperty("parallelism", bestScaleOut);
                            String jobId = ctx.cm.restartJob(job.getJobId(), body);
                            job.setJobId(jobId).setScaleOut(bestScaleOut).addTs(ctx.cm.getLatestTs(job.getJobId()));
                        }
                        else LOG.info(String.format("No better scale out found, staying with %d", job.getScaleOut()));
                    }
                    catch (Exception e) { LOG.info("Problem optimizing target job with message: " + e); }
                }
            });
            return EXECUTE;
        }
    },
    EXECUTE {

        public Graph runStage(Context context) throws Exception {

            // wait for the length of the experiment runtime
            LOG.info(Arrays.toString(context.failures.toArray()));
            context.et.register(new Listener(context.failures, () -> {

                context.job.addTs(context.cm.getLatestTs(context.job.getJobId()));
                String taskManager = context.cm.getTaskManagers(context.job.getJobId()).iterator().next();
                LOG.info("Injecting delay into taskManager: " + taskManager);
                context.cm.injectDelay(taskManager);
            }));
            context.et.start(context.expLen);

            // gather metrics and stop jobs
            long firstTs = context.job.getFirstTs();
            long currTs = context.cm.getLatestTs(context.job.getJobId());
            context.job.addTs(currTs);

            LOG.info("Gathering metrics and stopping job with configuration: " + context.job);
            String dirPath = String.format("%s/%s", context.dataPath, context.expId);
            Map<String, TimeSeries> metrics = new HashMap<>();
            Map<String, TimeSeries> allCpu = context.cm.getAllCpuLoad("pod", firstTs, currTs);
            Map<String, TimeSeries> allMem = context.cm.getAllMemUsed("pod", firstTs, currTs);
            metrics.put(String.format("%s_%s_cpuLoad.out", dirPath, context.job.name), TimeSeries.asyncMerge(context.executor, List.copyOf(allCpu.values())));
            metrics.put(String.format("%s_%s_memUsed.out", dirPath, context.job.name), TimeSeries.asyncMerge(context.executor, List.copyOf(allMem.values())));
            metrics.put(String.format("%s_%s_bckPres.out", dirPath, context.job.name), context.cm.getBckPres(context.job.name, firstTs, currTs));
            metrics.put(String.format("%s_%s_consLag.out", dirPath, context.job.name), context.cm.getConsLag(context.job.name, firstTs, currTs));
            metrics.put(String.format("%s_%s_workRate.out", dirPath, context.job.name), context.cm.getWorkload(context.executor, firstTs, currTs));
            metrics.put(String.format("%s_%s_thrRate.out", dirPath, context.job.name), context.cm.getThr(context.job.name, firstTs, currTs));
            metrics.put(String.format("%s_%s_latency.out", dirPath, context.job.name), context.cm.getLat(context.job.name, firstTs, currTs));
            metrics.put(String.format("%s_%s_chkDur.out", dirPath, context.job.name), context.cm.getChkDur(context.job.name, firstTs, currTs));
            metrics.put(String.format("%s_%s_numWorkers.out", dirPath, context.job.name), context.cm.getNumWorkers(context.job.getFirstTs(), context.job.getLastTs()));
            metrics.put(String.format("%s_%s_numRestarts.out", dirPath, context.job.name), context.cm.getNumRestarts(context.job.name, context.job.getFirstTs(), context.job.getLastTs()));
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
