package io.github.melin.flink.rest;

import io.github.melin.flink.rest.client.api.ApiClient;
import io.github.melin.flink.rest.client.api.FlinkRestClientApi;
import io.github.melin.flink.rest.client.model.*;

public class FlinkClientTest {
    public static void main(String[] args) {
        ApiClient apiClient = new ApiClient();
        FlinkRestClientApi flinkApi = new FlinkRestClientApi(apiClient);

        flinkApi.getApiClient().setBasePath("http://x.x.x.x:42300/v1");
        MultipleJobsDetails jobs = flinkApi.getJobsOverview();

        System.out.println(jobs);

        JobStatusInfo jobStatusInfo = flinkApi.getJobStatusInfo("68f9fd7069ace1f36ea5e01cc0c58719");

        System.out.println(jobStatusInfo);

        //flinkApi.getApiClient().setDebugging(true);
        CheckpointTriggerRequestBody checkpointTriggerRequestBody = new CheckpointTriggerRequestBody();
        checkpointTriggerRequestBody.checkpointType(CheckpointType.FULL);
        TriggerResponse triggerResponse = flinkApi.triggerCheckpoint("68f9fd7069ace1f36ea5e01cc0c58719",
                checkpointTriggerRequestBody);
        System.out.println(triggerResponse);
    }
}
