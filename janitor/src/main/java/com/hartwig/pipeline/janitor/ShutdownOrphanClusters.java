package com.hartwig.pipeline.janitor;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toMap;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

import com.google.api.services.dataproc.Dataproc;
import com.google.api.services.dataproc.model.Cluster;
import com.google.api.services.dataproc.model.Job;
import com.google.api.services.dataproc.model.Operation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShutdownOrphanClusters implements CleanupTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(ShutdownOrphanClusters.class);

    private final Dataproc dataproc;
    private Map<Cluster, List<Job>> runningClusters;

    ShutdownOrphanClusters(final Dataproc dataproc) {
        this.dataproc = dataproc;
    }

    @Override
    public void execute(Arguments arguments) {
        LOGGER.info("Checking for clusters which are running with no running jobs.");
        try {
            List<Cluster> clusters =
                    dataproc.projects().regions().clusters().list(arguments.project(), arguments.region()).execute().getClusters();
            Map<String, List<Job>> jobs = dataproc.projects()
                    .regions()
                    .jobs()
                    .list(arguments.project(), arguments.region())
                    .execute()
                    .getJobs()
                    .stream()
                    .filter(ResourceState::running)
                    .collect(groupingBy(job -> job.getPlacement().getClusterName()));
            if (clusters != null) {
                runningClusters = clusters.stream()
                        .filter(ResourceState::running)
                        .collect(toMap(cluster -> cluster,
                                cluster -> jobs.getOrDefault(cluster.getClusterName(), Collections.emptyList())));

                for (Map.Entry<Cluster, List<Job>> entry : runningClusters.entrySet()) {
                    if (entry.getValue().isEmpty()) {
                        LOGGER.info("Cluster [{}] is running but has no running Spark jobs. Deleting this cluster.",
                                entry.getKey().getClusterName());
                        waitForOperationComplete(dataproc.projects()
                                .regions()
                                .clusters()
                                .delete(arguments.project(), arguments.region(), entry.getKey().getClusterName())
                                .execute());

                        LOGGER.info("Cluster [{}] has been deleted", entry.getKey().getClusterName());
                    }
                }
            }

        } catch (Exception e) {
            LOGGER.error("Unable to sweep running clusters for orphans.", e);
        }
    }

    private void waitForOperationComplete(Operation operation) throws IOException {
        waitForComplete(operation,
                op1 -> op1.getDone() != null && op1.getDone(),
                () -> dataproc.projects().regions().operations().get(operation.getName()).execute(),
                op -> op.getMetadata().get("description").toString());
    }

    private <T> void waitForComplete(T operation, Predicate<T> isDone, Poll<T> poll, Function<T, String> description) throws IOException {
        boolean operationComplete = isDone.test(operation);
        while (!operationComplete) {
            sleep();
            LOGGER.debug("Operation [{}] not complete, waiting...", description.apply(operation));
            operation = poll.poll();
            operationComplete = isDone.test(operation);
        }
    }

    private void sleep() {
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            Thread.interrupted();
        }
    }

    Map<Cluster, List<Job>> getRunningClusters() {
        return runningClusters;
    }

    private interface Poll<T> {
        T poll() throws IOException;
    }
}
