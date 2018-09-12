package com.hartwig.pipeline.cluster;

import java.io.IOException;
import java.util.Collections;
import java.util.function.Function;
import java.util.function.Predicate;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.dataproc.Dataproc;
import com.google.api.services.dataproc.model.Cluster;
import com.google.api.services.dataproc.model.ClusterConfig;
import com.google.api.services.dataproc.model.InstanceGroupConfig;
import com.google.api.services.dataproc.model.Job;
import com.google.api.services.dataproc.model.JobPlacement;
import com.google.api.services.dataproc.model.JobStatus;
import com.google.api.services.dataproc.model.NodeInitializationAction;
import com.google.api.services.dataproc.model.Operation;
import com.google.api.services.dataproc.model.SparkJob;
import com.google.api.services.dataproc.model.SubmitJobRequest;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.bootstrap.Arguments;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GoogleDataprocCluster implements SampleCluster {

    private static final String MACHINE_TYPE_URI = "n1-standard-32";
    private static final int NUM_WORKERS = 4;
    private static final String APPLICATION_NAME = "sample-dataproc-cluster";
    private final Logger LOGGER = LoggerFactory.getLogger(GoogleDataprocCluster.class);
    private String clusterName;
    private Dataproc dataproc;
    private final GoogleCredentials credential;

    public GoogleDataprocCluster(final GoogleCredentials credential) {
        this.credential = credential;
    }

    @Override
    public void start(Sample sample, Arguments arguments) throws IOException {
        this.clusterName = "sample-" + sample.name().toLowerCase() + "-1";
        dataproc = new Dataproc.Builder(new NetHttpTransport(),
                JacksonFactory.getDefaultInstance(),
                new HttpCredentialsAdapter(credential)).setApplicationName(APPLICATION_NAME).build();
        Dataproc.Projects.Regions.Clusters clusters = dataproc.projects().regions().clusters();
        Cluster existing = findExistingCluster(arguments);
        if (existing == null) {
            Operation createCluster = clusters.create(arguments.project(),
                    arguments.region(),
                    cluster(clusterConfig(masterConfig(), workerConfig(), arguments.runtimeBucket()), clusterName)).execute();
            LOGGER.info("Starting Google Dataproc cluster with name [{}]. This may take a minute or two...", clusterName);
            waitForOperationComplete(createCluster);
            LOGGER.info("Cluster started.");
        } else {
            LOGGER.info("Cluster [{}] already exists, using this cluster to submit pipeline", clusterName);
        }
    }

    @Override
    public void submit(SparkJobDefinition jobDefinition, Arguments arguments) throws IOException {
        LOGGER.info("Submitting spark job to cluster [{}]", clusterName);
        Job job = dataproc.projects()
                .regions()
                .jobs().submit(arguments.project(), arguments.region(),
                        new SubmitJobRequest().setJob(new Job().setPlacement(new JobPlacement().setClusterName(clusterName))
                                .setSparkJob(new SparkJob().setProperties(SparkProperties.asMap()).setMainClass(jobDefinition.mainClass())
                                        .setJarFileUris(Collections.singletonList(jobDefinition.jarLocation())))))
                .execute();
        Job completed = waitForComplete(job,
                j -> j.getStatus() != null && (j.getStatus().getState().equals("ERROR") || j.getStatus().getState().equals("DONE")
                        || j.getStatus().getState().equals("CANCELLED")),
                () -> dataproc.projects()
                        .regions()
                        .jobs()
                        .get(arguments.project(), arguments.region(), job.getReference().getJobId())
                        .execute(),
                GoogleDataprocCluster::jobStatus);
        LOGGER.info("Spark job is complete with status [{}] details [{}]",
                completed.getStatus().getState(),
                completed.getStatus().getDetails());
    }

    @Override
    public void stop(Arguments arguments) throws IOException {
        if (dataproc == null) {
            throw new IllegalStateException(
                    "No Dataproc instance available to stop running cluster. Did you forget to call start() before stop()?");
        }
        Operation deleteCluster =
                dataproc.projects().regions().clusters().delete(arguments.project(), arguments.region(), clusterName).execute();
        LOGGER.info("Deleting cluster [{}]. This may take a minute or two...", clusterName);
        waitForOperationComplete(deleteCluster);
        LOGGER.info("Cluster deleted");

    }

    private static String jobStatus(final Job job) {
        String template = "Status [%s] Substatus [%s] Details [%s]";
        JobStatus status = job.getStatus();
        return String.format(template, status.getState(), stringOrNone(status.getSubstate()), stringOrNone(status.getDetails()));
    }

    private static String stringOrNone(final String string) {
        return string != null ? string : "None";
    }

    private Cluster findExistingCluster(Arguments arguments) throws IOException {
        try {
            return dataproc.projects().regions().clusters().get(arguments.project(), arguments.region(), clusterName).execute();
        } catch (GoogleJsonResponseException e) {
            return null;
        }
    }

    private void waitForOperationComplete(Operation operation) throws IOException {
        waitForComplete(operation,
                op1 -> op1.getDone() != null && op1.getDone(),
                () -> dataproc.projects().regions().operations().get(operation.getName()).execute(),
                op -> op.getMetadata().get("description").toString());
    }

    private <T> T waitForComplete(T operation, Predicate<T> isDone, Poll<T> poll, Function<T, String> description) throws IOException {
        boolean operationComplete = false;
        while (!operationComplete) {
            sleep();
            LOGGER.info("Operation [{}] not complete, waiting...", description.apply(operation));
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

    private InstanceGroupConfig masterConfig() {
        return new InstanceGroupConfig().setMachineTypeUri(MACHINE_TYPE_URI);
    }

    private ClusterConfig clusterConfig(final InstanceGroupConfig masterConfig, final InstanceGroupConfig workerConfig,
            final String bucket) {
        return new ClusterConfig().setMasterConfig(masterConfig)
                .setWorkerConfig(workerConfig)
                .setConfigBucket(bucket)
                .setInitializationActions(Collections.singletonList(new NodeInitializationAction().setExecutableFile(
                        "gs://pipeline5-runtime/cluster-init/install-bwa.sh")));
    }

    private InstanceGroupConfig workerConfig() {
        return new InstanceGroupConfig().setMachineTypeUri(MACHINE_TYPE_URI).setNumInstances(NUM_WORKERS);
    }

    private com.google.api.services.dataproc.model.Cluster cluster(final ClusterConfig clusterConfig, final String clusterName) {
        return new com.google.api.services.dataproc.model.Cluster().setClusterName(clusterName).setConfig(clusterConfig);
    }

    private interface Poll<T> {
        T poll() throws IOException;
    }
}
