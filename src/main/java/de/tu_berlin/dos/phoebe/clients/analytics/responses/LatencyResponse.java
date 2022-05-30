package de.tu_berlin.dos.phoebe.clients.analytics.responses;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

import java.util.List;

public class  LatencyResponse {

    public static class Item {

        @SerializedName("scale_out")
        @Expose
        public int scaleOut;
        @SerializedName("latency")
        @Expose
        public double avgLat;
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
                    ",avgLat:" + avgLat +
                    ",isBest:" + isBest +
                    ",isValid:" + isValid +
                    '}';
        }
    }

    public Item current;
    public List<Item> candidates;
    public double slope;

    public int getBestScaleOutByMinLatency() {

        double avgLat = this.current.avgLat;
        int scaleOut = this.current.scaleOut;
        for (Item item : this.candidates) {

            if (Math.abs((avgLat - item.avgLat) / item.avgLat) < 0.05 && item.scaleOut < scaleOut) {

                avgLat = item.avgLat;
                scaleOut = item.scaleOut;
            }
            else if (0.05 < Math.abs((avgLat - item.avgLat) / item.avgLat) && item.avgLat < avgLat) {

                avgLat = item.avgLat;
                scaleOut = item.scaleOut;
            }
        }
        System.out.println("getBestScaleOutByMinLatency: avgLat " + avgLat + ", scaleOut " + scaleOut);
        return scaleOut;
    }

    public int getBestScaleOut(int maxScaleOut) {

        int bestScaleOut = maxScaleOut;
        for (Item item : this.candidates) {
            // find the best scale out in candidates
            if (item.isBest) bestScaleOut = item.scaleOut;
        }
        if (0 < this.slope) {
            // workload increasing but current is still valid for max
            if (this.current.isValid) bestScaleOut = current.scaleOut;
        }
        return bestScaleOut;
    }
    
    public double getLatencyByScaleOut(int scaleOut) {

        double latency = 0D;
        for (Item item : this.candidates) {

            if (item.scaleOut == scaleOut) {

                latency = item.avgLat;
                break;
            }
        }
        return latency;
    }

    @Override
    public String toString() {
        return "{" +
                "current:" + current +
                ",candidates:" + candidates +
                '}';
    }
}
