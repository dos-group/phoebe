package de.tu_berlin.dos.phoebe.managers;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import de.tu_berlin.dos.phoebe.clients.analytics.AnalyticsClient;
import de.tu_berlin.dos.phoebe.clients.analytics.responses.LatencyResponse;
import de.tu_berlin.dos.phoebe.clients.analytics.responses.RecTimeResponse;
import de.tu_berlin.dos.phoebe.clients.flink.FlinkClient;
import de.tu_berlin.dos.phoebe.clients.generators.GeneratorsClient;
import de.tu_berlin.dos.phoebe.clients.kubernetes.Helm;
import de.tu_berlin.dos.phoebe.clients.kubernetes.Helm.CommandBuilder;
import de.tu_berlin.dos.phoebe.clients.kubernetes.Helm.CommandBuilder.Command;
import de.tu_berlin.dos.phoebe.clients.kubernetes.K8sClient;
import de.tu_berlin.dos.phoebe.clients.prometheus.PrometheusClient;
import de.tu_berlin.dos.phoebe.execution.Job;
import de.tu_berlin.dos.phoebe.structures.PropertyTree;
import de.tu_berlin.dos.phoebe.structures.TimeSeries;
import de.tu_berlin.dos.phoebe.utils.FileManager;
import io.fabric8.chaosmesh.v1alpha1.DelaySpec;
import io.fabric8.chaosmesh.v1alpha1.DelaySpecBuilder;
import io.fabric8.chaosmesh.v1alpha1.NetworkChaosBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class ClientsManager implements AutoCloseable {

    /******************************************************************************
     * CLASS VARIABLES
     ******************************************************************************/

    private static final Logger LOG = LogManager.getLogger(ClientsManager.class);

    private static final String TO_REPLACE = "TO_REPLACE";
    private static final String BACKPRESSURE = "flink_taskmanager_job_task_isBackPressured";
    private static final String LATENCY = "flink_taskmanager_job_task_operator_myLatencyHistogram";
    private static final String THROUGHPUT = "flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_records_consumed_rate";
    private static final String CHECKPOINT_DURATIONS = "flink_jobmanager_job_lastCheckpointDuration";
    private static final String CONSUMER_LAG = "flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_records_lag_max";
    private static final String CPU_LOAD = "flink_taskmanager_Status_JVM_CPU_Load";
    private static final String MEMORY_USED = "flink_taskmanager_Status_JVM_Memory_Heap_Used";
    private static final String NUM_TASKMANAGERS = "flink_jobmanager_numRegisteredTaskManagers";
    private static final String NUM_RESTARTS = "flink_jobmanager_job_numRestarts";
    private static final String UPTIME = "flink_jobmanager_job_uptime";

    /******************************************************************************
     * CLASS BEHAVIOURS
     ******************************************************************************/

    public static ClientsManager create(
            String namespace, String flinkUrl, String jarPath, String sourceRegex,
            String savepointPath, String promUrl, String pyBaseUrl, String generatorList) throws Exception {

        return new ClientsManager(namespace, flinkUrl, jarPath, sourceRegex, savepointPath, promUrl, pyBaseUrl, generatorList);
    }

    /******************************************************************************
     * INSTANCE STATE
     ******************************************************************************/

    public final Gson gson = new GsonBuilder().disableHtmlEscaping().serializeNulls().create();
    public final String namespace;
    public final K8sClient k8sClient;
    public final FlinkClient flink;
    public final Path jarPath;
    public final String sourceRegex;
    public final String savepointPath;
    public final PrometheusClient prom;
    public final AnalyticsClient analyticsClient;
    public final List<GeneratorsClient> generatorsClients = new ArrayList<>();

    /******************************************************************************
     * CONSTRUCTOR(S)
     ******************************************************************************/

    public ClientsManager(
            String namespace, String flinkUrl, String jarPath, String sourceRegex,
            String savepointPath, String promUrl, String pyBaseUrl, String generatorList) {

        this.namespace = namespace;
        this.k8sClient = new K8sClient();
        this.prom = new PrometheusClient(promUrl, gson);
        this.flink = new FlinkClient(flinkUrl, gson);
        this.jarPath = Paths.get(jarPath);
        this.sourceRegex = sourceRegex;
        this.savepointPath = savepointPath;
        this.analyticsClient = new AnalyticsClient(pyBaseUrl, gson);
        Arrays.asList(generatorList.split(",")).forEach(baseUrl -> {

            this.generatorsClients.add(new GeneratorsClient(baseUrl, gson));
        });
    }

    /******************************************************************************
     * INSTANCE BEHAVIOUR
     ******************************************************************************/

    public void createNamespace() throws Exception {

        this.k8sClient.deleteNamespace(this.namespace);
        this.k8sClient.createOrReplaceNamespace(this.namespace);
    }

    public void deleteNamespace() throws Exception {

        this.k8sClient.deleteNamespace(this.namespace);
    }

    public void deployHelm(PropertyTree deployment) throws Exception {

        LOG.info("Helm deploy: " + deployment);
        CommandBuilder builder = CommandBuilder.builder();
        builder.setCommand(Command.INSTALL);
        builder.setName(deployment.key);
        if (deployment.exists("chart")) {

            String path = FileManager.GET.path(deployment.find("chart").value);
            builder.setChart(path);
        }
        else builder.setChart(deployment.key);
        if (deployment.exists("repo")) builder.setFlag("--repo", deployment.find("repo").value);
        if (deployment.exists("version")) builder.setFlag("--version", deployment.find("version").value);
        if (deployment.exists("values")) {
            // replace temporary placeholders strings in yaml files with namespace
            File orig = FileManager.GET.resource(deployment.find("values").value, File.class);
            String contents = FileManager.GET.toString(orig).replaceAll(TO_REPLACE, this.namespace);
            File temp = FileManager.GET.tmpWithContents(contents);
            builder.setFlag("--values", temp.getAbsolutePath());
        }
        builder.setNamespace(this.namespace);
        Helm.get.execute(builder.build());
    }

    public void deployYaml(PropertyTree deployment) throws Exception {

        LOG.info("Yaml deploy: " + deployment);
        deployment.forEach(file -> {
            // replace temporary placeholders strings in yaml files with namespace
            File orig = FileManager.GET.resource(file.value, File.class);
            String contents = FileManager.GET.toString(orig).replaceAll(TO_REPLACE, this.namespace);
            File temp = FileManager.GET.tmpWithContents(contents);
            this.k8sClient.createOrReplaceResources(this.namespace, temp.getAbsolutePath());
        });
    }

    public List<String> getPodNames(String labelKey, String labelVal) {

        return k8sClient.getPods(this.namespace, labelKey, labelVal);
    }

    public String uploadJar() throws Exception {

        JsonObject json = this.flink.uploadJar(this.jarPath);
        String[] parts = json.get("filename").getAsString().split("/");
        return parts[parts.length - 1];
    }

    public List<String> getJobs() throws Exception {

        JsonObject json = this.flink.getJobs();
        List<String> jobs = new ArrayList<>();
        json.get("jobs").getAsJsonArray().forEach(e -> {

            jobs.add(e.getAsJsonObject().get("id").getAsString());
        });
        return jobs;
    }

    public String startJob(String jarId, JsonObject programArgs) throws Exception {

        JsonObject json = this.flink.startJob(jarId, programArgs);
        return json.get("jobid").getAsString();
    }

    public String restartJob(String jobId, JsonObject programArgsList) throws Exception {

        // Initiate job save
        AtomicReference<String> location = new AtomicReference<>();
        JsonObject body = new JsonObject();
        body.addProperty("target-directory", this.savepointPath);
        body.addProperty("cancel-job", false);
        int attempts = 0;
        while (true) {
            LOG.info("Saving job");
            String requestId = this.flink.saveJob(jobId, body).get("request-id").getAsString();
            // wait until save is completed
            JsonObject response = this.flink.checkStatus(jobId, requestId);
            while (!"COMPLETED".equals(response.get("status").getAsJsonObject().get("id").getAsString())) {

                new CountDownLatch(1).await(100, TimeUnit.MILLISECONDS);
                response = this.flink.checkStatus(jobId, requestId);
            }
            // ensure save completed successfully
            if (response.has("operation") &&
                response.get("operation").getAsJsonObject().has("location")) {
                LOG.info("Save complete");
                // retrieve savepoint location and stop job
                location.set(response.get("operation").getAsJsonObject().get("location").getAsString());
                this.stopJob(jobId);
                break;
            }
            else attempts++;
            if (attempts > 60) throw new IllegalStateException("Unable to restart, throwing exception");
            new CountDownLatch(1).await(100, TimeUnit.MILLISECONDS);
        }
        LOG.info("Restarting job from savepoint with:" + programArgsList.toString());
        // restart job with new scaleout
        programArgsList.addProperty("savepointPath", location.get());
        return this.flink.startJob(Job.jarId, programArgsList).get("jobid").getAsString();
    }

    public void stopJob(String jobId) throws Exception {

        this.flink.stopJob(jobId);
    }

    public long getLastCheckpoint(String jobId) throws Exception {

        JsonObject response = this.flink.getCheckpoints(jobId);
        long lastCkp = response
            .getAsJsonObject("latest").get("completed")
            .getAsJsonObject().get("latestAckTimestamp").getAsLong();
        return (long) Math.ceil(lastCkp / 1000.0);
    }

    public List<String> getVertices(String jobId) throws Exception {

        JsonObject response = this.flink.getVertices(jobId);
        JsonArray nodes = response.getAsJsonObject("plan").getAsJsonArray("nodes");
        List<String> operatorIds = new ArrayList<>();
        nodes.forEach(vertex -> operatorIds.add(vertex.getAsJsonObject().get("id").getAsString()));
        return operatorIds;
    }

    public Set<String> getTaskManagers(String jobId) throws Exception {

        List<String> vertices = this.getVertices(jobId);
        Set<String> taskManagers = new HashSet<>();
        for (String id : vertices) {

            JsonObject response = this.flink.getTaskManagers(jobId, id);
            JsonArray arr = response.getAsJsonArray("taskmanagers");
            arr.forEach(taskManager -> taskManagers.add(taskManager.getAsJsonObject().get("taskmanager-id").getAsString()));
        }
        return taskManagers;
    }

    public long getLatestTs(String jobId) throws Exception {

        long now = this.flink.getLatestTs(jobId).get("now").getAsLong();
        return (long) Math.ceil(now / 1000.0);
    }

    public int getScaleOut(String jobId) throws Exception {

        JsonObject response = this.flink.getJob(jobId);
        return response.get("vertices").getAsJsonArray().get(0).getAsJsonObject().get("parallelism").getAsInt();
    }

    public ClientsManager initGenerators() throws Exception {

        for (GeneratorsClient client : this.generatorsClients) {

            String status = client.status().getAsJsonObject().get("status").getAsString();
            while (!"READY".equalsIgnoreCase(status)) {

                client.stop();
                new CountDownLatch(1).await(1000, TimeUnit.MILLISECONDS);
                status = client.status().getAsJsonObject().get("status").getAsString();
            }
        }
        return this;
    }

    public ClientsManager startGenerators(JsonObject body) throws Exception {

        for (GeneratorsClient client : this.generatorsClients) {

            LOG.info(String.format("Generator started with parameters: %s", client.start(body).toString()));
        }
        return this;
    }

    public TimeSeries getWorkload(ExecutorService executor, long startTs, long stopTs) throws Exception {

        JsonObject body = new JsonObject();
        body.addProperty("startTs", startTs);
        body.addProperty("stopTs", stopTs);
        List<TimeSeries> workloads = new ArrayList<>();
        for (GeneratorsClient client : this.generatorsClients) {

            workloads.add(gson.fromJson(client.workload(body), TimeSeries.class));
        }
        return TimeSeries.asyncMerge(executor, workloads);
    }

    public void injectFailure(String taskManager) throws Exception {

        this.k8sClient.execCommandOnPod(taskManager, this.namespace, "sh", "-c", "kill 1");
    }

    public void injectDelay(String taskManager) {

        String name = "delay-" + taskManager;
        DelaySpec delay = new DelaySpecBuilder().withLatency("60000ms").withCorrelation("100").withJitter("0ms").build();
        this.k8sClient.createOrReplaceNetworkChaos(this.namespace, new NetworkChaosBuilder()
            .withNewMetadata().withName(name).endMetadata()
            .withNewSpec()
            .withAction("delay")
            .withMode("one")
            .withNewSelector().withFieldSelectors(Map.of("metadata.name", taskManager)).endSelector()
            .withDelay(delay)
            .withDuration("60s")
            .endSpec()
            .build());
    }

    public boolean isBckPres(double bckPresPer, double workDone, double workNeeded, TimeSeries latency, TimeSeries consLag) throws Exception {

        JsonObject body = new JsonObject();
        body.add("latency", gson.toJsonTree(latency));
        body.add("consLag", gson.toJsonTree(consLag));
        JsonObject response = this.analyticsClient.commonRegression(body);
        double latencyGrad = response.get("latency").getAsJsonObject().get("slope").getAsDouble();
        double consLagGrad = response.get("consLag").getAsJsonObject().get("slope").getAsDouble();
        LOG.info("workDone: " + workDone + ", workNeeded: " + workNeeded + ", latencyGrad: " + latencyGrad + ", consLagGrad: " + consLagGrad);
        return (bckPresPer == 1) || (0.0 < bckPresPer && 1 < latencyGrad && 1 < consLagGrad);
    }

    public TimeSeries getBckPres(String jobName, long startTs, long stopTs) throws Exception {

        String query = String.format(
            "sum(%s{job_name=\"%s\",task_name=~\"%s\",namespace=\"%s\"})",
            BACKPRESSURE, jobName, this.sourceRegex, this.namespace);
        return this.prom.queryRange(query, startTs, stopTs);
    }

    public TimeSeries getLat(String jobName, long startTs, long stopTs) throws Exception {

        String query = String.format(
            "sum(%s{job_name=\"%s\",quantile=\"0.95\",namespace=\"%s\"})/" +
            "count(%s{job_name=\"%s\",quantile=\"0.95\",namespace=\"%s\"})",
            LATENCY, jobName, this.namespace, LATENCY, jobName, this.namespace);
        return this.prom.queryRange(query, startTs, stopTs);
    }

    public TimeSeries getThr(String jobName, long startTs, long stopTs) throws Exception {

        String query = String.format(
            "sum(%s{job_name=\"%s\",namespace=\"%s\"})",
            THROUGHPUT, jobName, this.namespace);
        return this.prom.queryRange(query, startTs, stopTs);
    }

    public TimeSeries getChkDur(String jobName, long startTs, long stopTs) throws Exception {

        String query = String.format(
            "%s{job_name=\"%s\",namespace=\"%s\"}",
            CHECKPOINT_DURATIONS, jobName, this.namespace);
        return this.prom.queryRange(query, startTs, stopTs);
    }

    public TimeSeries getConsLag(String jobName, long startTs, long stopTs) throws Exception {

        String query = String.format(
            "sum(%s{job_name=\"%s\",namespace=\"%s\"})/count(%s{job_name=\"%s\",namespace=\"%s\"})",
            CONSUMER_LAG, jobName, this.namespace, CONSUMER_LAG, jobName, this.namespace);
        return this.prom.queryRange(query, startTs, stopTs);
    }

    public TimeSeries getCpuLoad(String taskManager, long startTs, long stopTs) throws Exception {

        String query = String.format(
            "%s{pod=\"%s\",namespace=\"%s\"}",
            CPU_LOAD, taskManager, this.namespace);
        return this.prom.queryRange(query, startTs, stopTs);
    }

    public TimeSeries getMemUsed(String taskManager, long startTs, long stopTs) throws Exception {

        String query = String.format(
            "%s{pod=\"%s\",namespace=\"%s\"}",
            MEMORY_USED, taskManager, this.namespace);
        return this.prom.queryRange(query, startTs, stopTs);
    }

    public TimeSeries getNumWorkers(long startTs, long stopTs) throws Exception {

        String query = String.format(
            "%s{namespace=\"%s\"}",
            NUM_TASKMANAGERS, this.namespace);
        LOG.info(query);
        return this.prom.queryRange(query, startTs, stopTs);
    }

    public TimeSeries getNumRestarts(String jobName, long startTs, long stopTs) throws Exception {

        String query = String.format(
            "%s{job_id=\"%s\",namespace=\"%s\"}",
            NUM_RESTARTS, jobName, this.namespace);
        return this.prom.queryRange(query, startTs, stopTs);
    }

    public long getUptime(String jobName, long startTs, long stopTs) throws Exception {

        String query = String.format(
            "%s{job_name=\"%s\",namespace=\"%s\"}",
            UPTIME, jobName, this.namespace);

        TimeSeries ts = this.prom.queryRange(query, startTs, stopTs);
        return (ts.getLast() != null && ts.getLast().value != null) ? ts.getLast().value.longValue() / 1000 : 0;
    }

    public Map<String, TimeSeries> getAllCpuLoad(String metric, long startTs, long stopTs) throws Exception {

        String query = String.format(
            "%s{namespace=\"%s\"}",
            CPU_LOAD, this.namespace);
        return this.prom.queryRange(query, metric, startTs, stopTs);
    }

    public Map<String, TimeSeries> getAllMemUsed(String metric, long startTs, long stopTs) throws Exception {

        String query = String.format(
            "%s{namespace=\"%s\"}",
            MEMORY_USED, this.namespace);
        return this.prom.queryRange(query, metric, startTs, stopTs);
    }

    public double regressionPredict(TimeSeries ts, double x) throws Exception {

        JsonObject body = new JsonObject();
        body.add("ts", gson.toJsonTree(ts));
        JsonObject response = this.analyticsClient.commonRegression(body);
        double slope = response.get("ts").getAsJsonObject().get("slope").getAsDouble();
        double intercept = response.get("ts").getAsJsonObject().get("intercept").getAsDouble();
        LOG.info("slope: " + slope + ", intercept: " + intercept + ", x: " + x + ", y: " + ((slope * x) + intercept));
        return (slope * x) + intercept;
    }

    public void capacityTrain(ExecutorService executor, List<Integer> scaleOuts, List<Double> maxThrRates, String job) throws Exception {

        JsonObject body = new JsonObject();
        body.add("scale_outs", gson.toJsonTree(scaleOuts));
        body.add("max_throughput_rates", gson.toJsonTree(maxThrRates));
        body.addProperty("job", job);

        String taskHash = this.analyticsClient.capacityTrain(body).get("task_hash").getAsString();
        CountDownLatch latch = new CountDownLatch(1);

        executor.submit(() -> {
            try {
                boolean isRunning = true;
                while (isRunning) {

                    isRunning = this.analyticsClient.commonTasks(taskHash);
                    LOG.info("Training capacity model, is still running: " + isRunning);
                    Thread.sleep(1000);
                }
                latch.countDown();
            }
            catch (Exception e) { throw new IllegalStateException(e.getMessage()); }
        });
        latch.await();
    }

    public void latencyTrain(
            ExecutorService executor, int scaleOut, double avgThr,
            double avgLat, boolean append, String job) throws Exception {

        this.latencyTrain(executor, List.of(scaleOut), List.of(avgThr), List.of(avgLat), append, job);
    }

    public void latencyTrain(
            ExecutorService executor, List<Integer> scaleOuts, List<Double> avgThr,
            List<Double> avgLat, boolean append, String job) throws Exception {

        JsonObject body = new JsonObject();
        body.add("scale_outs", gson.toJsonTree(scaleOuts));
        body.add("throughput_rates", gson.toJsonTree(avgThr));
        body.add("latencies", gson.toJsonTree(avgLat));
        body.addProperty("append", append);
        body.addProperty("job", job);

        String taskHash = this.analyticsClient.latencyTrain(body).get("task_hash").getAsString();
        CountDownLatch latch = new CountDownLatch(1);
        executor.submit(() -> {
            try {
                boolean isRunning = true;
                while (isRunning) {

                    isRunning = this.analyticsClient.commonTasks(taskHash);
                    LOG.info("Training latency model, is still running: " + isRunning);
                    Thread.sleep(1000);
                }
                latch.countDown();
            }
            catch (Exception e) { throw new IllegalStateException(e.getMessage()); }
        });
        // wait until models are trained
        latch.await();
    }

    public LatencyResponse latencyPredict(
            double throughputRate, int minScaleOut, int maxScaleOut, int optimalLatency) throws IOException {

        JsonObject body = new JsonObject();
        body.addProperty("throughput_rate", throughputRate);
        body.addProperty("min_scale_out", minScaleOut);
        body.addProperty("max_scale_out", maxScaleOut);
        body.addProperty("optimal_latency", optimalLatency);
        return this.analyticsClient.latencyPredict(body);
    }

    public LatencyResponse latencyEvaluate(RecTimeResponse response, String job) throws IOException {

        JsonObject body = new JsonObject();
        body.add("current", gson.toJsonTree(response.current));
        body.add("candidates", gson.toJsonTree(response.candidates));
        body.addProperty("predicted_throughput_rate", response.predThr);
        body.addProperty("job", job);
        return this.analyticsClient.latencyEvaluate(body);
    }

    public RecTimeResponse recTimePredict(
            int minScaleOut, int maxScaleOut, TimeSeries workload, int currScaleOut, int predPeriod,
            float downtime, float lastCheckpoint, float maxRecTimeConst, String job) throws IOException {

        JsonObject body = new JsonObject();
        body.addProperty("min_scale_out", minScaleOut);
        body.addProperty("max_scale_out", maxScaleOut);
        body.add("workload", gson.toJsonTree(workload));
        body.addProperty("scale_out", currScaleOut);
        body.addProperty("bin_count", 5);
        body.addProperty("prediction_period_in_s", predPeriod);
        body.addProperty("downtime", downtime);
        body.addProperty("last_checkpoint", lastCheckpoint);
        body.addProperty("max_recovery_time", maxRecTimeConst);
        body.addProperty("job", job);
        return this.analyticsClient.recTimePredict(body);
    }

    public String twresTrain(List<Integer> scaleOuts, List<Double> avgThrRates, String generatorType) throws IOException {

        JsonObject body = new JsonObject();
        body.add("scale_outs", gson.toJsonTree(scaleOuts));
        body.add("throughput_rates", gson.toJsonTree(avgThrRates));
        body.addProperty("job", generatorType);
        return this.analyticsClient.twresTrain(body).get("task_hash").getAsString();
    }

    public int twresPredict(
            TimeSeries workload, int evalInt, double avgLat, double maxLatConst,
            int scaleOut, int minScaleOut, int maxScaleOut, String generatorType) throws IOException {

        JsonObject body = new JsonObject();
        body.add("workload", gson.toJsonTree(workload));
        body.addProperty("avg_latency", (int) avgLat);
        body.addProperty("max_latency_constraint", (int) maxLatConst);
        body.addProperty("scale_out", scaleOut);
        body.addProperty("time_window_interval", evalInt);
        body.addProperty("min_scale_out", minScaleOut);
        body.addProperty("max_scale_out", maxScaleOut);
        body.addProperty("job", generatorType);
        return this.analyticsClient.twresPredict(body).get("scale_out").getAsInt();
    }

    @Override
    public void close() throws Exception {

        this.k8sClient.close();
    }
}
