package io.github.melin.flink.rest;

import io.github.melin.flink.rest.client.api.ApiClient;
import io.github.melin.flink.rest.client.api.FlinkApi;
import io.github.melin.flink.rest.client.model.JobIdsWithStatusOverview;

public class FlinkClientTest {
    public static void main(String[] args) {
        ApiClient apiClient = new ApiClient();
        FlinkApi flinkApi = new FlinkApi(apiClient);

        flinkApi.getApiClient().setBasePath("http://ip:8088/proxy/application_1688224869950_0118/");
        JobIdsWithStatusOverview jobs = flinkApi.getJobs();

        System.out.println(jobs);
    }
}
