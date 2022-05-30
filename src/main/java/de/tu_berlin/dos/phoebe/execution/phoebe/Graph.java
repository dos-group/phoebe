package de.tu_berlin.dos.phoebe.execution.phoebe;

import com.google.gson.JsonObject;
import de.tu_berlin.dos.phoebe.clients.analytics.responses.LatencyResponse;
import de.tu_berlin.dos.phoebe.clients.analytics.responses.RecTimeResponse;
import de.tu_berlin.dos.phoebe.execution.Job;
import de.tu_berlin.dos.phoebe.execution.Workload;
import de.tu_berlin.dos.phoebe.managers.DataManager.Profile;
import de.tu_berlin.dos.phoebe.structures.Observation;
import de.tu_berlin.dos.phoebe.structures.SequenceFSM;
import de.tu_berlin.dos.phoebe.structures.TimeSeries;
import de.tu_berlin.dos.phoebe.utils.Evaluate;
import de.tu_berlin.dos.phoebe.utils.EventTimer.Listener;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static java.util.stream.Collectors.*;

public enum Graph implements SequenceFSM<Context, Graph> {

    START {

        public Graph runStage(Context ctx) {

            return DEPLOY;
        }
    },
    DEPLOY {

        public Graph runStage(Context ctx) throws Exception {

            // re-initialize namespace
            ctx.cm.createNamespace();
            // deploy to k8s using helm
            ctx.k8s.find("HELM").forEach(ctx.cm::deployHelm);
            // deploy to k8s using yaml
            ctx.k8s.find("YAML").forEach(ctx.cm::deployYaml);
            // waiting for systems to come online
            new CountDownLatch(1).await(30, TimeUnit.SECONDS);
            // upload job jar
            LOG.info("Uploading Jar...");
            Job.jarId = ctx.cm.uploadJar();
            LOG.info("Jar uploaded with id: " + Job.jarId);
            return MODEL;
        }
    },
    PROFILE {

        public Graph runStage(Context ctx) throws Exception {

            ctx.dm.initProfiles(ctx.expId, ctx.genType, ctx.cleanDb);

            // Start profiling jobs
            for (Job job : ctx.profiles) {

                if (!job.isActive()) continue;
                job.setJobId(ctx.cm.startJob(Job.jarId, job.getProgramArgsList()));
                LOG.info("Started latency profiling job with configuration: " + job.getProgramArgsList().toString());
            }

            // generate workload rates for evaluation interval and record metrics
            int generatorsCount = ctx.cm.generatorsClients.size();
            // loop through increasing workloads until max capacity of all profiles is found
            while (ctx.profiles.stream().anyMatch(Job::isActive)) {
                // retrieve the next workload for profiling run
                Workload workload = ctx.workIterator.next();
                LOG.info("Beginning profiling run with configuration: " + workload.toString());
                // ensure all load generators are in READY state and start generating
                ctx.cm.initGenerators().startGenerators(workload.getBody(generatorsCount));

                // execute loop profiling run for the length of evaluation interval
                ctx.et.start(300 + ctx.avgWindow);
                // on conclusion of profiling run, process profiling data for each job
                for (Job job : ctx.profiles) {
                    // ensure stopped jobs are ignored
                    if (!job.isActive()) continue;
                    // collect metrics based on current timestamp
                    long stopTs = ctx.cm.getLatestTs(job.getJobId());
                    long startTs = stopTs - ctx.avgWindow;
                    double avgLat = ctx.cm.getLat(job.name, startTs, stopTs).avg();
                    //job.avgLatTs.getObservations().add(new Observation(stopTs, avgLat));
                    double avgThr = ctx.cm.getThr(job.name, startTs, stopTs).avg();
                    //job.avgThrTs.getObservations().add(new Observation(stopTs, avgThr));
                    ctx.dm.addProfile(ctx.expId, ctx.genType, job.name, job.getScaleOut(), avgLat, avgThr, 0, startTs, stopTs);
                }
                // gather all active scale outs and metrics associated with them for evaluation
                List<Integer> activeScaleOuts = ctx.profiles.stream().filter(Job::isActive).map(Job::getScaleOut).toList();
                Map<Integer, TimeSeries> candidates = new LinkedHashMap<>();
                ctx.dm.getProfiles(ctx.expId, ctx.genType).stream()
                    .filter(p -> activeScaleOuts.contains(p.getScaleOut()))
                    .collect(groupingBy(Profile::getScaleOut, mapping(Function.identity(), toList())))
                    .forEach((scaleOut, profiles) -> {
                        TimeSeries ts = TimeSeries.create();
                        for (int i = 0; i < profiles.size(); i++) {
                            ts.getObservations().add(i, new Observation(profiles.get(i).stopTs, profiles.get(i).avgLat));
                        }
                        candidates.put(scaleOut, ts);
                    });

                // find valid latencies using clustering algorithm
                Map<Integer, Double> valid;
                if (2 < candidates.size()) valid = Evaluate.clustering(candidates, 2.0f);
                else valid = Evaluate.regression(candidates, 2.0f);
                valid.forEach((key, val) -> LOG.info("key: " + key + ", val: " + val));
                // stop unfinished jobs
                for (Job job : ctx.profiles) {

                    if (!job.isActive()) continue;
                    else if (!valid.containsKey(job.getScaleOut())) {
                        job.isActive(false);
                        ctx.cm.stopJob(job.getJobId());
                        LOG.info("Stopping latency profiling job with configuration: " + job.getProgramArgsList().toString());
                    }
                }
            }
            // determine max capacity by putting jobs into backpressure from the earliest offset
            for (Job job : ctx.profiles) {

                job.extraArgs.put("offset", "earliest");
                job.setJobId(ctx.cm.startJob(Job.jarId, job.getProgramArgsList()));
                LOG.info("Started capacity profiling job with configuration: " + job.getProgramArgsList().toString());
                ctx.et.start(300 + ctx.avgWindow);
                long stopTs = ctx.cm.getLatestTs(job.getJobId());
                long startTs = stopTs - ctx.avgWindow;
                double avgLat = ctx.cm.getLat(job.name, startTs, stopTs).avg();
                double avgThr = ctx.cm.getThr(job.name, startTs, stopTs).avg();
                ctx.dm.addProfile(ctx.expId, ctx.genType, job.name, job.getScaleOut(), avgLat, avgThr, 1, startTs, stopTs);
                ctx.cm.stopJob(job.getJobId());
                LOG.info("Stopping capacity profiling job with configuration: " + job.getProgramArgsList().toString());
            }
            // stop load generators
            ctx.cm.initGenerators();
            return MODEL;
        }
    },
    MODEL {

        public Graph runStage(Context ctx) throws Exception {

            // extract data from database
            List<Profile> profiles = ctx.dm.getProfiles(ctx.expId, ctx.genType);
            // train capacity model based on profiling data
            List<Integer> scaleOuts = new ArrayList<>();
            List<Double> maxAvgThr = new ArrayList<>();
            for (Profile profile : profiles) {

                if (profile.isBckPres) {

                    scaleOuts.add(profile.scaleOut);
                    maxAvgThr.add(profile.avgThr);
                }
            }
            LOG.info("Training capacity model");
            ctx.cm.capacityTrain(ctx.executor, scaleOuts, maxAvgThr, ctx.genType);
            // train latency model based on profiling data
            scaleOuts = new ArrayList<>();
            List<Double> avgThr = new ArrayList<>();
            List<Double> avgLat = new ArrayList<>();
            for (Profile profile : profiles) {

                if (!profile.isBckPres) {

                    scaleOuts.add(profile.scaleOut);
                    avgThr.add(profile.avgThr);
                    avgLat.add(profile.avgLat);
                }
            }
            LOG.info("Training latency model");
            ctx.cm.latencyTrain(ctx.executor, scaleOuts, avgThr, avgLat, false, ctx.genType);
            return INITIALIZE;
        }
    },
    INITIALIZE {

        @Override
        public Graph runStage(Context ctx) throws Exception {

            // setup generators
            JsonObject body = new JsonObject();
            body.addProperty("brokerList", ctx.brokerList);
            body.addProperty("topic", ctx.tarConsTopic);
            body.addProperty("generatorType", ctx.genType);
            body.addProperty("limiterType", ctx.limiterType);
            body.addProperty("limiterMaxNoise", ctx.limiterMaxNoise);
            // based on supplied generator type, set properties
            switch (ctx.limiterType) {

                case "SINE" -> {

                    int generatorsCount = ctx.cm.generatorsClients.size();
                    body.addProperty("amplitude", ctx.amplitude / generatorsCount);
                    body.addProperty("verticalPhase", ctx.verticalPhase / generatorsCount);
                    body.addProperty("period", ctx.period);
                }
                case "DATASET" -> {

                    body.addProperty("fileName", ctx.fileName);
                }
                default -> throw new IllegalStateException("Unknown generator type: " + ctx.genType);
            }
            LOG.info("Start workload generation with configuration: " + body);
            ctx.cm.initGenerators().startGenerators(body);

            // start the target job
            ctx.job.setJobId(ctx.cm.startJob(Job.jarId, ctx.job.getProgramArgsList()));
            ctx.job.addTs(ctx.cm.getLatestTs(ctx.job.getJobId()));
            LOG.info("Started job with configuration: " + ctx.job.getProgramArgsList().toString());
            return OPTIMIZE;
        }
    },
    OPTIMIZE {

        public Graph runStage(Context ctx) throws Exception {

            // execute optimization step in separate thread
            ctx.dm.initPredictions(ctx.expId, ctx.genType,true);
            ctx.executor.submit(() -> {

                Job job = ctx.job;
                while (!Thread.currentThread().isInterrupted()) {

                    try {
                        // ensure job uptime and time since last update is over 10 minutes
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
                        double avgThr = ctx.cm.getThr(job.name, (currTs - ctx.avgWindow), currTs).avg();
                        LOG.info("ScaleOut: " + currScaleOut + ", avgLat: " + avgLat + ", avgThr: " + avgThr);
                        // updating the latency model
                        ctx.cm.latencyTrain(ctx.executor, currScaleOut, avgThr, avgLat, true, ctx.genType);
                        // perform predictions and evaluation of latency and recovery time
                        RecTimeResponse recTimeRes = ctx.cm.recTimePredict(
                            ctx.minScaleOut, ctx.maxScaleOut, workload, currScaleOut,
                            600, (10 + /*80ADS*/ /*120cars*/ 100), 10, ctx.maxRecTimeConst, ctx.genType);
                        LOG.info("recTimeRes: " + recTimeRes);
                        int bestScaleOutByMaxRecTime = recTimeRes.getBestScaleOutByMaxRecTime();
                        LatencyResponse latRes = ctx.cm.latencyEvaluate(recTimeRes, ctx.genType);
                        LOG.info("latRes: " + latRes);
                        int bestScaleOutByMinLatency = latRes.getBestScaleOutByMinLatency();
                        int maxScaleOut = Math.max(bestScaleOutByMaxRecTime, bestScaleOutByMinLatency);
                        // determine if a new scale out should be chosen
                        int bestScaleOut = latRes.getBestScaleOut(maxScaleOut);
                        // determine if new scale-out was found which is also not only scaling down by one
                        if (bestScaleOut != currScaleOut && bestScaleOut != currScaleOut - 1 && bestScaleOut != currScaleOut - 2) {

                            LOG.info(String.format("Rescale initiated from %d -> %d", currScaleOut, bestScaleOut));
                            JsonObject body = job.getProgramArgsList();
                            body.addProperty("parallelism", bestScaleOut);
                            String jobId = ctx.cm.restartJob(job.getJobId(), body);
                            job.setJobId(jobId).setScaleOut(bestScaleOut).addTs(ctx.cm.getLatestTs(job.getJobId()));
                            LOG.info("Rescale completed with new job settings: " + job);
                            double predAvgLat = latRes.getLatencyByScaleOut(bestScaleOut);
                            double predRecTime = recTimeRes.getRecTimeByScaleOut(bestScaleOut);
                            LOG.info("Writing predictions to database with: " + job.getScaleOut() + ", " + predAvgLat + ", " + predRecTime);
                            ctx.dm.addPrediction(ctx.expId, ctx.genType, currTs, job.getScaleOut(), avgThr, predAvgLat, predRecTime);
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

        @Override
        public Graph runStage(Context ctx) throws Exception {

            // wait for the length of the experiment runtime
            LOG.info(Arrays.toString(ctx.failures.toArray()));
            // register failures for experiment runs
            ctx.et.register(new Listener(ctx.failures, () -> {

                ctx.job.addTs(ctx.cm.getLatestTs(ctx.job.getJobId()));
                String taskManager = ctx.cm.getTaskManagers(ctx.job.getJobId()).iterator().next();
                LOG.info("Injecting delay into taskManager: " + taskManager);
                ctx.cm.injectDelay(taskManager);
            }));
            // execute experiment for length of time
            ctx.et.start(ctx.expLen);

            // gather metrics and stop jobs
            long firstTs = ctx.job.getFirstTs();
            long currTs = ctx.cm.getLatestTs(ctx.job.getJobId());
            ctx.job.addTs(currTs);

            LOG.info("Gathering metrics and stopping job with configuration: " + ctx.job);
            String dirPath = String.format("%s/%s", ctx.dataPath, ctx.expId);
            Map<String, TimeSeries> metrics = new HashMap<>();
            metrics.put(String.format("%s_%s_workRate.out", dirPath, ctx.job.name), ctx.cm.getWorkload(ctx.executor, firstTs, currTs));
            metrics.put(String.format("%s_%s_thrRate.out", dirPath, ctx.job.name), ctx.cm.getThr(ctx.job.name, firstTs, currTs));
            metrics.put(String.format("%s_%s_latency.out", dirPath, ctx.job.name), ctx.cm.getLat(ctx.job.name, firstTs, currTs));
            // write metrics to file
            CountDownLatch latch = new CountDownLatch(3);
            metrics.forEach((name, timeSeries) -> {

                ctx.executor.submit(() -> {

                    try { TimeSeries.toCSV(name, timeSeries, "timestamp|value", "|"); }
                    catch (IOException e) { e.printStackTrace(); }
                    finally { latch.countDown(); }
                });
            });
            latch.await();
            // stop job
            ctx.cm.stopJob(ctx.job.getJobId());
            // stop load generators
            ctx.cm.initGenerators();

            return STOP;
        }
    },
    STOP {

        public Graph runStage(Context ctx) throws Exception {

            ctx.close();
            return this;
        }
    };

    public static void start(String propsFile) throws Exception {

        Graph.START.run(Graph.class, Context.create(propsFile));
    }
}
