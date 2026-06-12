# flink-client

This library provides a Java client for managing Apache Flink via the [Monitoring REST API](https://ci.apache.org/projects/flink/flink-docs-stable/monitoring/rest_api.html).

The client is generated with [Swagger Codegen](https://swagger.io/tools/swagger-codegen/) from an OpenAPI specification file.

## Build
```
export GPG_TTY=$(tty)
mvn clean deploy -Pdeploy
```

## How to get the binaries

The library is available in Maven Central Repository, and GitHub.

If you are using Maven, add this dependency to your POM:

    <dependency>
        <groupId>io.github.melin</groupId>
        <artifactId>flink-rest-client</artifactId>
        <version>2.2.0</version>
    </dependency>

## Documentation

Create the Flink client:

    FlinkRestClientApi api = new FlinkRestClientApi();

Configure host and port of the server:

    api.getApiClient().setBasePath("http://localhost:8081");

Configure socket timeouts:

    api.getApiClient().getHttpClient().setConnectTimeout(20000, TimeUnit.MILLISECONDS)
    api.getApiClient().getHttpClient().setWriteTimeout(30000, TimeUnit.MILLISECONDS)
    api.getApiClient().getHttpClient().setReadTimeout(30000, TimeUnit.MILLISECONDS)

Optionally enable debugging:

    api.getApiClient().setIsDebugging(true)

Get Flink cluster configuration:

    DashboardConfiguration config = api.showConfig();

Show list of uploaded jars:

    JarListInfo jars = api.listJars();

Upload a jar which contain a Flink job:

    JarUploadResponseBody result = api.uploadJar(new File("flink-job.jar"));

Run an uploaded jar which some arguments:

    JarRunResponseBody response = api.runJar("bf4afb3b-d662-435e-b465-5ddb40d68379_flink-job.jar", true, null, "--INPUT A --OUTPUT B", null, "your-main-class", null);

Get status of all jobs:

    JobIdsWithStatusOverview jobs = api.getJobs();

Get details of a job:

    JobDetailsInfo details = api.getJobDetails("f370f5421e5254eed8d6fc6673829c83");

Terminate a job:

    api.terminateJob("f370f5421e5254eed8d6fc6673829c83", "cancel");

For all the remaining operations see documentation of Monitoring REST API or see [OpenAPI specification file](https://github.com/nextbreakpoint/flink-client/blob/master/flink-openapi.yaml).

      
