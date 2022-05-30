package de.tu_berlin.dos.phoebe.execution;

import com.google.gson.JsonObject;

public class Workload {

    public static class Iterator {

        private final String generatorType;
        private final String brokerList;
        private final String topic;
        private final int stepSize;

        private int currRate;

        private Iterator(String generatorType, String brokerList, String topic, int stepSize, int firstRate) {

            this.generatorType = generatorType;
            this.brokerList = brokerList;
            this.topic = topic;
            this.stepSize = stepSize;
            this.currRate = firstRate;
        }

        public Workload next() {

            Workload loader = new Workload(this.generatorType, this.brokerList, this.topic, this.currRate);
            this.currRate += this.stepSize;
            return loader;
        }
    }

    public static Iterator iterator(String generatorType, String brokerList, String topic, int stepSize, int firstRate) {

        return new Iterator(generatorType, brokerList, topic, stepSize, firstRate);
    }

    private final String generatorType;
    private final String brokerList;
    private final String topic;
    private final int rate;

    private Workload(String generatorType, String brokerList, String topic, int rate) {

        this.generatorType = generatorType;
        this.brokerList = brokerList;
        this.topic = topic;
        this.rate = rate;
    }

    public int getRate() {

        return rate;
    }

    public JsonObject getBody(int generatorsCount) {

        JsonObject body = new JsonObject();
        body.addProperty("generatorType", this.generatorType);
        body.addProperty("brokerList", this.brokerList);
        body.addProperty("limiterType", "SINE");
        body.addProperty("limiterMaxNoise", "0");
        body.addProperty("amplitude", "0");
        body.addProperty("verticalPhase", this.rate / generatorsCount);
        body.addProperty("topic", this.topic);
        return body;
    }

    @Override
    public String toString() {

        return "{" +
                "rate:" + this.rate +
                ",brokerList:'" + this.brokerList + '\'' +
                ",topic:'" + this.topic + '\'' +
                ",generatorType:'" + this.generatorType + '\'' +
                '}';
    }
}
