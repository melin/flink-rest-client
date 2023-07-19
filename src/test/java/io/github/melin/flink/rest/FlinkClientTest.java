package io.github.melin.flink.rest;

import com.gitee.melin.bee.util.JsonUtils;
import io.github.melin.flink.rest.client.api.ApiClient;
import io.github.melin.flink.rest.client.api.FlinkRestClientApi;
import io.github.melin.flink.rest.client.model.*;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.util.Map;

public class FlinkClientTest {
    public static void main(String[] args) {
        RestTemplate restTemplate = new RestTemplate();
        ApiClient apiClient = new ApiClient(restTemplate);
        FlinkRestClientApi flinkApi = new FlinkRestClientApi(apiClient);
        // flinkApi.getApiClient().setDebugging(true);

        String uri = "http://cdh3:42300/v1";
        flinkApi.getApiClient().setBasePath(uri);
        MultipleJobsDetails jobs = flinkApi.getJobsOverview();
        System.out.println(jobs);

        String jobId = "c5addaa24f08c8b0dccba7a6d4e0b6f2";
        JobStatusInfo jobStatusInfo = flinkApi.getJobStatusInfo(jobId);
        System.out.println(jobStatusInfo);

        ResponseEntity<CheckpointConfigInfo> checkpointConfigInfo = flinkApi.getCheckpointConfigWithHttpInfo(jobId);
        System.out.println(checkpointConfigInfo.getBody());

        SavepointTriggerRequestBody requestBody = new SavepointTriggerRequestBody();
        requestBody.setTargetDirectory("/tmp/" + jobId);
        TriggerResponse triggerResponse = flinkApi.triggerSavepoint(jobId, requestBody);
        System.out.println(triggerResponse);

        String httpUri = uri + "/jobs/" + jobId + "/checkpoints";

        final String json = restTemplate.getForObject(httpUri, String.class);
        Map<String, Object> checkpoint = JsonUtils.toJavaMap(json);
        System.out.println(checkpoint);
    }
}
