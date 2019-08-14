package com.hartwig.pipeline.execution.dataproc;

import static java.util.Arrays.asList;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.dataproc.v1beta2.Dataproc;
import com.google.api.services.dataproc.v1beta2.model.Cluster;
import com.google.api.services.dataproc.v1beta2.model.ClusterConfig;
import com.google.api.services.dataproc.v1beta2.model.Job;
import com.google.api.services.dataproc.v1beta2.model.JobPlacement;
import com.google.api.services.dataproc.v1beta2.model.JobReference;
import com.google.api.services.dataproc.v1beta2.model.Operation;
import com.google.api.services.dataproc.v1beta2.model.SparkJob;
import com.google.api.services.dataproc.v1beta2.model.SubmitJobRequest;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.storage.GoogleStorageStatusCheck;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.storage.StatusCheck;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GoogleDataproc implements SparkExecutor {

    private static final String APPLICATION_NAME = "sample-dataproc-cluster";
    private static final Logger LOGGER = LoggerFactory.getLogger(GoogleDataproc.class);

    private final Dataproc dataproc;
    private final NodeInitialization nodeInitialization;
    private final Arguments arguments;
    private final GoogleStorageStatusCheck statusCheck;

    GoogleDataproc(final Dataproc dataproc, final NodeInitialization nodeInitialization, final Arguments arguments,
            GoogleStorageStatusCheck statusCheck) {
        this.dataproc = dataproc;
        this.nodeInitialization = nodeInitialization;
        this.arguments = arguments;
        this.statusCheck = statusCheck;
    }

    public static GoogleDataproc from(final GoogleCredentials credential, final NodeInitialization nodeInitialization,
            final Arguments arguments, ResultsDirectory resultsDirectory) {
        return new GoogleDataproc(new Dataproc.Builder(new NetHttpTransport(),
                JacksonFactory.getDefaultInstance(),
                new HttpCredentialsAdapter(credential)).setApplicationName(APPLICATION_NAME).build(),
                nodeInitialization,
                arguments,
                new GoogleStorageStatusCheck(resultsDirectory));
    }

    @Override
    public PipelineStatus submit(RuntimeBucket runtimeBucket, SparkJobDefinition jobDefinition) {
        try {
            PipelineStatus status;
            String jobIdAndClusterName = jobIdAndClusterName(runtimeBucket, jobDefinition);
            LOGGER.info("Submitting spark job [{}] to cluster [{}]", jobDefinition.name(), jobIdAndClusterName);
            final Job job =
                    findExistingJob(arguments, jobIdAndClusterName, jobDefinition.name(), runtimeBucket).orElseGet(() -> submittedJob(
                            jobDefinition,
                            runtimeBucket,
                            jobIdAndClusterName));
            LOGGER.info("Found existing job: [{}]", job);
            if (!isDone(job, jobDefinition.name(), runtimeBucket)) {
                Job completed = waitForComplete(job,
                        j -> j.getStatus() != null && (j.getStatus().getState().equals("ERROR") || j.getStatus()
                                .getState()
                                .equals("CANCELLED") || isDone(j, jobDefinition.name(), runtimeBucket)),
                        () -> dataproc.projects()
                                .regions()
                                .jobs()
                                .get(arguments.project(), arguments.region(), job.getReference().getJobId())
                                .execute(),
                        GoogleDataproc::jobStatus);
                stop(jobIdAndClusterName);
                if (completed.getStatus().getState().equals("DONE")) {
                    status = PipelineStatus.SUCCESS;
                } else {
                    status = PipelineStatus.FAILED;
                }
            } else {
                status = PipelineStatus.SKIPPED;
            }
            LOGGER.info("Spark job [{}] is complete with status [{}]", jobDefinition.name(), status);
            return status;
        } catch (IOException e) {
            LOGGER.error("Exception while interacting with Google Dataproc APIs", e);
            return PipelineStatus.FAILED;
        }
    }

    @NotNull
    private String jobIdAndClusterName(final RuntimeBucket runtimeBucket, final SparkJobDefinition jobDefinition) {
        String untrimmed = runtimeBucket.runId() + "-" + jobDefinition.name().toLowerCase();
        return untrimmed.substring(0, Math.min(untrimmed.length(), 50));
    }

    private boolean isDone(final Job job, final String jobName, final RuntimeBucket runtimeBucket) {
        String state = job.getStatus().getState();
        if (state.equals("DONE")) {
            List<StatusCheck.Status> finalStatuses = asList(StatusCheck.Status.SUCCESS, StatusCheck.Status.FAILED);
            LOGGER.info("Checker status: [{}]", getStatus(jobName, runtimeBucket));
            return finalStatuses.contains(getStatus(jobName, runtimeBucket));
        }
        return false;
    }

    private StatusCheck.Status getStatus(final String jobName, final RuntimeBucket runtimeBucket) {
        return statusCheck.check(runtimeBucket, jobName);
    }

    private void start(final DataprocPerformanceProfile performanceProfile, final RuntimeBucket runtimeBucket, final Arguments arguments,
            final String clusterName) throws IOException {
        Dataproc.Projects.Regions.Clusters clusters = dataproc.projects().regions().clusters();
        Cluster existing = findExistingCluster(arguments, clusterName);
        if (existing == null) {
            createCluster(performanceProfile, runtimeBucket, arguments, clusterName, clusters);
        } else if (existing.getStatus().getState().equals("ERROR")) {
            waitForOperationComplete(clusters.delete(arguments.project(), arguments.region(), clusterName).execute());
            createCluster(performanceProfile, runtimeBucket, arguments, clusterName, clusters);
        } else {
            LOGGER.debug("Cluster [{}] already exists, using this cluster to run pipeline", clusterName);
        }
    }

    private void createCluster(final DataprocPerformanceProfile performanceProfile, final RuntimeBucket runtimeBucket,
            final Arguments arguments, final String clusterName, final Dataproc.Projects.Regions.Clusters clusters) throws IOException {
        ClusterConfig clusterConfig = GoogleClusterConfig.from(runtimeBucket, nodeInitialization, performanceProfile, arguments).config();
        Operation createCluster = clusters.create(arguments.project(), arguments.region(), cluster(clusterConfig, clusterName)).execute();
        LOGGER.debug("Starting Google Dataproc cluster with name [{}]. This may take a minute or two...", clusterName);
        waitForOperationComplete(createCluster);
        LOGGER.debug("Cluster started.");
    }

    private Job submittedJob(final SparkJobDefinition jobDefinition, final RuntimeBucket runtimeBucket, final String naturalJobId) {
        try {
            start(jobDefinition.performanceProfile(), runtimeBucket, arguments, naturalJobId);
            return dataproc.projects()
                    .regions()
                    .jobs()
                    .submit(arguments.project(),
                            arguments.region(),
                            new SubmitJobRequest().setJob(new Job().setPlacement(new JobPlacement().setClusterName(naturalJobId))
                                    .setReference(new JobReference().setJobId(naturalJobId))
                                    .setSparkJob(new SparkJob().setProperties(jobDefinition.sparkProperties())
                                            .setMainClass(jobDefinition.mainClass())
                                            .setArgs(jobDefinition.arguments())
                                            .setJarFileUris(Collections.singletonList(jobDefinition.jarLocation())))))
                    .execute();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void stop(String clusterName) throws IOException {
        Operation deleteCluster =
                dataproc.projects().regions().clusters().delete(arguments.project(), arguments.region(), clusterName).execute();
        LOGGER.debug("Deleting cluster [{}]. This may take a minute or two...", clusterName);
        waitForOperationComplete(deleteCluster);
        LOGGER.debug("Cluster deleted");
    }

    private static String jobStatus(final Job job) {
        String template = "Status [%s] Substatus [%s] Details [%s]";
        com.google.api.services.dataproc.v1beta2.model.JobStatus status = job.getStatus();
        return String.format(template, status.getState(), stringOrNone(status.getSubstate()), stringOrNone(status.getDetails()));
    }

    private static String stringOrNone(final String string) {
        return string != null ? string : "None";
    }

    private Cluster findExistingCluster(final Arguments arguments, final String clusterName) throws IOException {
        try {
            return dataproc.projects().regions().clusters().get(arguments.project(), arguments.region(), clusterName).execute();
        } catch (GoogleJsonResponseException e) {
            return null;
        }
    }

    private Optional<Job> findExistingJob(Arguments arguments, String jobId, String jobName, RuntimeBucket runtimeBucket)
            throws IOException {
        try {
            Job job = dataproc.projects().regions().jobs().get(arguments.project(), arguments.region(), jobId).execute();
            if (job != null) {
                if ("RUNNING".equals(job.getStatus().getState())) {
                    LOGGER.info("Job [{}] already existed and is running. Re-attaching to running job.", jobName);
                    return Optional.of(job);
                } else if (getStatus(jobName, runtimeBucket).equals(StatusCheck.Status.SUCCESS)) {
                    LOGGER.info("Job [{}] already existed and completed successfully. Skipping job.", jobName);
                    return Optional.of(job);
                } else {
                    LOGGER.info("Job [{}] already existed with status [{}]. Deleting and re-submitting.",
                            jobName, job.getStatus().getState());
                    dataproc.projects().regions().jobs().delete(arguments.project(), arguments.region(), jobId).execute();
                }
            }
            return Optional.empty();
        } catch (GoogleJsonResponseException e) {
            return Optional.empty();
        }
    }

    private void waitForOperationComplete(Operation operation) throws IOException {
        waitForComplete(operation,
                op1 -> op1.getDone() != null && op1.getDone(),
                () -> dataproc.projects().regions().operations().get(operation.getName()).execute(),
                op -> op.getMetadata().get("description").toString());
    }

    private <T> T waitForComplete(T operation, Predicate<T> isDone, Poll<T> poll, Function<T, String> description) throws IOException {
        boolean operationComplete = isDone.test(operation);
        while (!operationComplete) {
            sleep();
            LOGGER.debug("Operation [{}] not complete, waiting...", description.apply(operation));
            operation = poll.poll();
            operationComplete = isDone.test(operation);
        }
        return operation;
    }

    private void sleep() {
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            Thread.interrupted();
        }
    }

    private Cluster cluster(final ClusterConfig clusterConfig, final String clusterName) {
        return new Cluster().setClusterName(clusterName).setConfig(clusterConfig);
    }

    private interface Poll<T> {
        T poll() throws IOException;
    }
}
