package io.github.melin.flink.rest;

import io.github.melin.flink.rest.client.ApiClient;
import io.github.melin.flink.rest.client.api.FlinkRestClientApi;
import io.github.melin.flink.rest.client.model.JobDetails;
import io.github.melin.flink.rest.client.model.JobDetailsInfo;
import io.github.melin.flink.rest.client.model.JobDetailsVertexInfo;
import io.github.melin.flink.rest.client.model.JobStatus;
import io.github.melin.flink.rest.client.model.Metric;
import org.junit.jupiter.api.Test;
import org.springframework.web.client.RestTemplate;

import java.util.List;

public class FlinkCdcMetricsTest {

    @Test
    public void test() {
        RestTemplate restTemplate = new RestTemplate();
        ApiClient apiClient = new ApiClient(restTemplate);
        FlinkRestClientApi flinkApi = new FlinkRestClientApi(apiClient);
        // flinkApi.getApiClient().setDebugging(true);

        String uri = "http://172.18.5.44:32556/flink/flink-2dlybwsv3cofzrlj/v1";
        flinkApi.getApiClient().setBasePath(uri);

        List<JobDetails> jobDetails = flinkApi.getJobsOverview().getJobs();
        String jobId = jobDetails.get(0).getJid();
        System.out.println(jobId);

        JobStatus jobStatus = flinkApi.getJobStatusInfo(jobId).getStatus();
        System.out.println(jobStatus);

        JobDetailsInfo jobDetailsInfo = flinkApi.getJobDetails(jobId);
        List<JobDetailsVertexInfo> vertexInfos = jobDetailsInfo.getVertices();
        System.out.println("vertex count: " + vertexInfos.size());
        for (JobDetailsVertexInfo vertexInfo : vertexInfos) {
            System.out.println("    " + vertexInfo.getName());
        }

        System.out.println("\n--------" + vertexInfos.get(0).getName() + " metrics---------");
        List<Metric> metrics = flinkApi.getJobVertexMetrics(jobId, vertexInfos.get(0).getId(), null);
        metrics.stream().filter(metric -> metric.getId().contains("Flink_CDC_")).forEach(metric -> {
            // System.out.println(metric.getId());
            flinkApi.getJobVertexMetrics(jobId, vertexInfos.get(0).getId(), metric.getId()).forEach(metric1 -> {
                System.out.println("    " + metric1.getId() + ": " + metric1.getValue());
            });
        });

        int last = vertexInfos.size() - 1;
        System.out.println("\n--------" + vertexInfos.get(last).getName() + " metrics---------");
        metrics = flinkApi.getJobVertexMetrics(jobId, vertexInfos.get(last).getId(), null);
        metrics.stream().filter(metric -> metric.getId().contains("preCommit.paimon.table.")).forEach(metric -> {
            // System.out.println(metric.getId());
            flinkApi.getJobVertexMetrics(jobId, vertexInfos.get(last).getId(), metric.getId()).forEach(metric1 -> {
                System.out.println("    " + metric1.getId() + ": " + metric1.getValue());
            });
        });
    }
}

/*
上面代码输出内容如下：
a3ada79a5a0a89c3da77f941e7ada156
RUNNING
vertex count: 4
    Source: Flink CDC Event Source: mysql -> SchemaOperator -> PrePartition
    PostPartition -> Assign Bucket
    FlushEventAlignment -> Sink Writer: paimon Sink
    preCommit -> Sink Committer: paimon Sink

--------Source: Flink CDC Event Source: mysql -> SchemaOperator -> PrePartition metrics---------
    0.Source__Flink_CDC_Event_Source__mysql.numRecordsOut: 3
    0.Source__Flink_CDC_Event_Source__mysql.numRecordsOutPerSecond: 0.0
    0.Source__Flink_CDC_Event_Source__mysql.numBytesIn: 0
    0.Source__Flink_CDC_Event_Source__mysql.numBytesInPerSecond: 0.0
    0.Source__Flink_CDC_Event_Source__mysql.sourceIdleTime: 10740599
    0.Source__Flink_CDC_Event_Source__mysql.numRecordsIn: 28334
    0.Source__Flink_CDC_Event_Source__mysql.currentOutputWatermark: -9223372036854775808
    0.Source__Flink_CDC_Event_Source__mysql.currentFetchEventTimeLag: -1
    0.Source__Flink_CDC_Event_Source__mysql.numRecordsInErrors: 0
    0.Source__Flink_CDC_Event_Source__mysql.numBytesOut: 0
    0.Source__Flink_CDC_Event_Source__mysql.numRecordsInPerSecond: 2.0
    0.Source__Flink_CDC_Event_Source__mysql.numBytesOutPerSecond: 0.0
    0.Source__Flink_CDC_Event_Source__mysql.currentEmitEventTimeLag: -7234

--------preCommit -> Sink Committer: paimon Sink metrics---------
    0.preCommit.paimon.table.ods_paimon_sample.commit.lastCompactionOutputFileSize: 0
    0.preCommit.paimon.table.ods_paimon_sample.commit.lastGeneratedSnapshots: 1
    0.preCommit.paimon.table.ods_paimon_sample.commit.lastTableFilesDeleted: 0
    0.preCommit.paimon.table.ods_paimon_sample.commit.lastTableFilesAdded: 0
    0.preCommit.paimon.table.ods_paimon_sample.commit.lastPartitionsWritten: 0
    0.preCommit.paimon.table.ods_paimon_sample.commit.commitDuration_max: 1697
    0.preCommit.paimon.table.ods_paimon_sample.commit.lastTableFilesCommitCompacted: 0
    0.preCommit.paimon.table.ods_paimon_sample.commit.lastBucketsWritten: 0
    0.preCommit.paimon.table.ods_paimon_sample.commit.commitDuration_min: 505
    0.preCommit.paimon.table.ods_paimon_sample.commit.commitDuration_p75: 1478.0
    0.preCommit.paimon.table.ods_paimon_sample.commit.lastTableFilesAppended: 0
    0.preCommit.paimon.table.ods_paimon_sample.commit.lastDeltaRecordsCommitCompacted: 0
    0.preCommit.paimon.table.ods_paimon_sample.commit.lastCompactionInputFileSize: 0
    0.preCommit.paimon.table.ods_paimon_sample.commit.commitDuration_p999: 1697.0
    0.preCommit.paimon.table.ods_paimon_sample.commit.lastChangelogRecordsCommitCompacted: 0
    0.preCommit.paimon.table.ods_paimon_sample.commit.lastCommitDuration: 576
    0.preCommit.paimon.table.ods_paimon_sample.commit.lastDeltaRecordsAppended: 0
    0.preCommit.paimon.table.ods_paimon_sample.commit.commitDuration_p98: 1697.0
    0.preCommit.paimon.table.ods_paimon_sample.commit.commitDuration_p99: 1697.0
    0.preCommit.paimon.table.ods_paimon_sample.commit.commitDuration_mean: 899.75
    0.preCommit.paimon.table.ods_paimon_sample.commit.commitDuration_p95: 1697.0
    0.preCommit.paimon.table.ods_paimon_sample.commit.commitDuration_p90: 1697.0
    0.preCommit.paimon.table.ods_paimon_sample.commit.lastChangelogFileCommitCompacted: 0
    0.preCommit.paimon.table.ods_paimon_sample.commit.lastChangelogFilesAppended: 0
    0.preCommit.paimon.table.ods_paimon_sample.commit.commitDuration_stddev: 548.4677899992548
    0.preCommit.paimon.table.ods_paimon_sample.commit.lastCommitAttempts: 1
    0.preCommit.paimon.table.ods_paimon_sample.commit.lastChangelogRecordsAppended: 0
    0.preCommit.paimon.table.ods_paimon_sample.commit.commitDuration_median: 698.5
 */
