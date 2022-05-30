package de.tu_berlin.dos.phoebe.clients.analytics;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import de.tu_berlin.dos.phoebe.clients.analytics.responses.LatencyResponse;
import de.tu_berlin.dos.phoebe.clients.analytics.responses.RecTimeResponse;
import okhttp3.OkHttpClient;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.gson.GsonConverterFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class AnalyticsClient {

    public final String baseUrl;
    public final AnalyticsRest service;

    public AnalyticsClient(String baseUrl, Gson gson) {

        this.baseUrl = "http://" + baseUrl + "/";
        OkHttpClient okHttpClient = new OkHttpClient.Builder()
                .connectTimeout(10, TimeUnit.MINUTES)
                .readTimeout(10, TimeUnit.MINUTES)
                .writeTimeout(10, TimeUnit.MINUTES)
                .build();
        Retrofit retrofit =
            new Retrofit.Builder()
                .baseUrl(this.baseUrl)
                .client(okHttpClient)
                .addConverterFactory(GsonConverterFactory.create(gson))
                .build();
        this.service = retrofit.create(AnalyticsRest.class);
    }

    public boolean commonTasks(String taskHash) throws Exception {

        Response<Boolean> response = this.service.commonTasks(taskHash).execute();
        if (response.isSuccessful() && response.body() != null) return response.body();
        else throw new IllegalStateException("Error performing common tasks: " + response.message());
    }

    public JsonObject commonRegression(JsonObject body) throws Exception {

        Response<JsonObject> response = this.service.commonRegression(body).execute();
        if (response.isSuccessful() && response.body() != null) return response.body();
        else throw new IllegalStateException("Error performing common regression: " + response.message());
    }

    public JsonObject capacityTrain(JsonObject body) throws IOException {

        Response<JsonObject> response = this.service.capacityTrain(body).execute();
        if (response.isSuccessful() && response.body() != null) return response.body();
        else throw new IllegalStateException("Error performing utilization train: " + response.message());
    }

    public JsonObject latencyTrain(JsonObject body) throws IOException {

        Response<JsonObject> response = this.service.latencyTrain(body).execute();
        if (response.isSuccessful() && response.body() != null) return response.body();
        else throw new IllegalStateException("Error performing latency train: " + response.message());
    }

    public LatencyResponse latencyPredict(JsonObject body) throws IOException {

        Response<LatencyResponse> response = this.service.latencyPredict(body).execute();
        if (response.isSuccessful() && response.body() != null) return response.body();
        else throw new IllegalStateException("Unable to predict using latency model: " + response.message());
    }

    public LatencyResponse latencyEvaluate(JsonObject body) throws IOException {

        Response<LatencyResponse> response = this.service.latencyEvaluate(body).execute();
        if (response.isSuccessful() && response.body() != null) return response.body();
        else throw new IllegalStateException("Unable to evaluate using latency model: " + response.message());
    }

    public RecTimeResponse recTimePredict(JsonObject body) throws IOException {

        Response<RecTimeResponse> response = this.service.recTimePredict(body).execute();
        if (response.isSuccessful() && response.body() != null) return response.body();
        else throw new IllegalStateException("Unable to predict using recovery time model: " + response.message());
    }

    public JsonObject recTimeEvaluate(JsonObject body) throws IOException {

        Response<JsonObject> response = this.service.recTimeEvaluate(body).execute();
        if (response.isSuccessful() && response.body() != null) return response.body();
        else throw new IllegalStateException("Unable to evaluate using recovery time model: " + response.message());
    }

    public JsonObject twresTrain(JsonObject body) throws IOException {

        Response<JsonObject> response = this.service.twresTrain(body).execute();
        if (response.isSuccessful() && response.body() != null) return response.body();
        else throw new IllegalStateException("Error performing TWRES train: " + response.message());
    }

    public JsonObject twresPredict(JsonObject body) throws IOException {

        Response<JsonObject> response = this.service.twresPredict(body).execute();
        if (response.isSuccessful() && response.body() != null) return response.body();
        else throw new IllegalStateException("Unable to predict using TWRES model: " + response.message());
    }
}
