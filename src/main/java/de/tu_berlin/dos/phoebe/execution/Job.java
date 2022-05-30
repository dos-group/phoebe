package de.tu_berlin.dos.phoebe.execution;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Job {

    public static String jarId;

    public final String name;
    public final String brokerList;
    public final String consumerTopic;
    public final String producerTopic;
    public final String entryClass;
    public final int chkInterval;
    public final Map<String, String> extraArgs = new HashMap<>();
    public final List<Long> tsList = new ArrayList<>();

    private int scaleOut;
    private String jobId;
    private boolean isActive = true;

    public Job(String name, String brokerList, String consumerTopic, String producerTopic, String entryClass, int chkInterval) {

        this.name = name;
        this.brokerList = brokerList;
        this.consumerTopic = consumerTopic;
        this.producerTopic = producerTopic;
        this.entryClass = entryClass;
        this.chkInterval = chkInterval;
    }

    public int getScaleOut() {

        return scaleOut;
    }

    public Job setScaleOut(int scaleOut) {

        this.scaleOut = scaleOut;
        return this;
    }

    public Job setJobId(String jobId) {

        this.jobId = jobId;
        return this;
    }

    public String getJobId() {

        return this.jobId;
    }

    public boolean isActive() {

        return this.isActive;
    }


    public void isActive(boolean isActive) {

        this.isActive = isActive;
    }

    public Job addTs(long timestamp) {

        this.tsList.add(timestamp);
        return this;
    }

    public long getFirstTs() {

        if (this.tsList.size() > 0) return this.tsList.get(0);
        else throw new IllegalStateException("Timestamps is empty for job: " + this);
    }

    public long getLastTs() {

        if (this.tsList.size() > 0) return this.tsList.get(this.tsList.size() - 1);
        else throw new IllegalStateException("Timestamps is empty for job: " + this);
    }

    public JsonObject getProgramArgsList() {

        JsonObject body = new JsonObject();
        JsonArray args = new JsonArray();
        args.add("--brokerList");
        args.add(this.brokerList);
        args.add("--consumerTopic");
        args.add(this.consumerTopic);
        args.add("--producerTopic");
        args.add(this.producerTopic);
        args.add("--jobName");
        args.add(this.name);
        args.add("--chkInterval");
        args.add(this.chkInterval);
        this.extraArgs.forEach((key, val) -> {
            args.add(String.format("--%s", key));
            args.add(val);
        });
        body.add("programArgsList", args);
        body.addProperty("parallelism", this.scaleOut);
        body.addProperty("entryClass", this.entryClass);
        return body;
    }

    @Override
    public String toString() {

        return "{jobName:'\"" + name + '\'' +
                ",scaleOut:" + scaleOut +
                ",brokerList:'" + brokerList + '\'' +
                ",consumerTopic:'" + consumerTopic + '\'' +
                ",producerTopic:'" + producerTopic + '\'' +
                ",chkInterval:" + chkInterval +
                ",jobName:'" + name + '\'' +
                ",jobId:'" + jobId + '\'' +
                ",isActive:" + isActive +
                ",tsList:[" + this.tsList.stream().map(String::valueOf).collect(Collectors.joining(",")) + "]" +
                '}';
    }
}
