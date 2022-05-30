package de.tu_berlin.dos.phoebe.clients.analytics.responses;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

import java.util.List;

public class RecTimeResponse {

    public static class Item {

        @SerializedName("scale_out")
        @Expose
        public int scaleOut;
        @SerializedName("recovery_time")
        @Expose
        public double recTime;
        @SerializedName("is_best")
        @Expose
        public boolean isBest;
        @SerializedName("is_valid")
        @Expose
        public boolean isValid;

        @Override
        public String toString() {
            return "{" +
                    "scaleOut:" + scaleOut +
                    ",recTime:" + recTime +
                    ",isBest:" + isBest +
                    ",isValid:" + isValid +
                    '}';
        }
    }

    public Item current;
    public List<Item> candidates;
    @SerializedName("predicted_throughput_rate")
    @Expose
    public double predThr;

    public int getBestScaleOutByMaxRecTime() {

        double recTime = this.current.recTime;
        int scaleOut = this.current.scaleOut;
        for (Item item : this.candidates) {

            if (Math.abs((recTime - item.recTime) / item.recTime) < 0.05 && item.scaleOut < scaleOut) {

                recTime = item.recTime;
                scaleOut = item.scaleOut;
            }
            else if (0.05 < Math.abs((recTime - item.recTime) / item.recTime) && item.recTime < recTime) {

                recTime = item.recTime;
                scaleOut = item.scaleOut;
            }
        }
        System.out.println("getBestScaleOutByMaxRecTime: recTime " + recTime + ", scaleOut " + scaleOut);
        return scaleOut;
    }

    public double getRecTimeByScaleOut(int scaleOut) {

        double recTime = 0D;
        for (Item item : this.candidates) {

            if (item.scaleOut == scaleOut) {

                recTime = item.recTime;
                break;
            }
        }
        return recTime;
    }

    @Override
    public String toString() {
        return "{" +
                "current:" + current +
                ",candidates:" + candidates +
                ",predThr:" + predThr +
                '}';
    }
}
