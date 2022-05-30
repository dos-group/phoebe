package de.tu_berlin.dos.phoebe.clients.analytics;

import com.google.gson.JsonObject;
import de.tu_berlin.dos.phoebe.clients.analytics.responses.LatencyResponse;
import de.tu_berlin.dos.phoebe.clients.analytics.responses.RecTimeResponse;
import retrofit2.Call;
import retrofit2.http.*;

public interface AnalyticsRest {

    @GET("/common/tasks/{task_hash}")
    Call<Boolean> commonTasks(
            @Path("task_hash") String taskHash
    );

    @Headers({
        "Accept: application/json",
        "Content-Type: application/json"
    })
    @POST("/common/regression")
    Call<JsonObject> commonRegression(
            @Body JsonObject body
    );

    @Headers({
        "Accept: application/json",
        "Content-Type: application/json"
    })
    @POST("recoverytime/training")
    Call<JsonObject> capacityTrain(
            @Body JsonObject body
    );

    @Headers({
        "Accept: application/json",
        "Content-Type: application/json"
    })
    @POST("latency/training")
    Call<JsonObject> latencyTrain(
            @Body JsonObject body
    );

    @Headers({
        "Accept: application/json",
        "Content-Type: application/json"
    })
    @POST("latency/prediction")
    Call<LatencyResponse> latencyPredict(
            @Body JsonObject body
    );

    @Headers({
            "Accept: application/json",
            "Content-Type: application/json"
    })
    @POST("latency/evaluation")
    Call<LatencyResponse> latencyEvaluate(
            @Body JsonObject body
    );

    @Headers({
            "Accept: application/json",
            "Content-Type: application/json"
    })
    @POST("recoverytime/prediction")
    Call<RecTimeResponse> recTimePredict(
            @Body JsonObject body
    );

    @Headers({
            "Accept: application/json",
            "Content-Type: application/json"
    })
    @POST("recoverytime/evaluation")
    Call<JsonObject> recTimeEvaluate(
            @Body JsonObject body
    );

    @Headers({
            "Accept: application/json",
            "Content-Type: application/json"
    })
    @POST("baselines/twres_training")
    Call<JsonObject> twresTrain(
            @Body JsonObject body
    );

    @Headers({
            "Accept: application/json",
            "Content-Type: application/json"
    })
    @POST("baselines/twres_prediction")
    Call<JsonObject> twresPredict(
            @Body JsonObject body
    );
}
