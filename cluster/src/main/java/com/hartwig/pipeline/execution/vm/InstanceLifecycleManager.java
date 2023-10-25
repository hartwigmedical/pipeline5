package com.hartwig.pipeline.execution.vm;

import static java.lang.String.format;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.compute.v1.Errors;
import com.google.cloud.compute.v1.Instance;
import com.google.cloud.compute.v1.InstancesClient;
import com.google.cloud.compute.v1.Metadata;
import com.google.cloud.compute.v1.Operation;
import com.google.cloud.compute.v1.Operation.Status;
import com.google.cloud.compute.v1.Zone;
import com.google.cloud.compute.v1.ZoneOperationsClient;
import com.google.cloud.compute.v1.ZonesClient;
import com.hartwig.pipeline.CommonArguments;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import net.jodah.failsafe.function.CheckedSupplier;

class InstanceLifecycleManager {
    public static final String DELETE_VM = "deleteVm";
    public static final String INSERT_VM = "insertVm";
    public static final String STOP_VM = "stopVm";
    public static final String SET_METADATA = "setMetadata";
    public static final String LIST_ZONES = "listZones";
    private static final String LIST_INSTANCES = "listInstances";
    private static final Logger LOGGER = LoggerFactory.getLogger(InstanceLifecycleManager.class);
    private static final int MAX_RETRIES = 4;

    private final String project;
    private final InstancesClient instances;
    private final ZonesClient zones;
    private final ZoneOperationsClient zoneOperations;
    private final String region;
    private final Integer pollInterval;

    InstanceLifecycleManager(final CommonArguments arguments, final InstancesClient compute, final ZonesClient zonesClient,
            final ZoneOperationsClient zoneOperations) {
        this.project = arguments.project();
        this.region = arguments.region();
        this.pollInterval = arguments.pollInterval();
        this.instances = compute;
        this.zones = zonesClient;
        this.zoneOperations = zoneOperations;
    }

    Optional<Instance> findExistingInstance(final String vmName) {
        for (String zone : fetchZones().stream().map(Zone::getName).collect(Collectors.toList())) {
            Iterable<Instance> instances =
                    executeSynchronouslyWithRetries(() -> this.instances.list(project, zone).iterateAll(), LIST_INSTANCES);
            if (instances != null) {
                for (Instance instance : instances) {
                    if (instance.getName().equals(vmName)) {
                        return Optional.of(instance);
                    }
                }
            }
        }
        return Optional.empty();
    }

    Operation deleteOldInstancesAndStart(final Instance instance, final String vmName, final String zone) {
        findExistingInstance(vmName).ifPresent(i -> {
            try {
                LOGGER.debug("Removing existing VM instance [{}] in [{}]", i.getName(), i.getZone());
                delete(i.getName(), i.getZone());
            } catch (Exception e) {
                throw new RuntimeException("Could not delete existing [" + vmName + "] instance", e);
            }
        });
        return executeSynchronously(instances.insertAsync(project, zone, instance), INSERT_VM, zone);
    }

    void delete(final String vm, final String zone) {
        String shortZone = zone.replaceAll(".*/", "");
        executeSynchronously(instances.deleteAsync(project, shortZone, vm), DELETE_VM, shortZone);
    }

    void stop(final String vm, final String zone) {
        executeSynchronously(instances.stopAsync(project, zone, vm), STOP_VM, zone);
    }

    String instanceStatus(final String vm, final String zone) {
        try {
            Instance found = instances.get(project, zone, vm);
            if (found != null) {
                return found.getStatus();
            } else {
                throw new IllegalStateException(format("Could not find instance [%s]", vm));
            }
        } catch (Exception e) {
            throw new RuntimeException("Unable to fetch instance status!", e);
        }
    }

    void disableStartupScript(final String vm, final String zone) throws IOException {
        String latestFingerprint = instances.get(project, zone, vm).getMetadata().getFingerprint();
        executeSynchronously(instances.setMetadataAsync(project, zone, vm, Metadata.newBuilder().setFingerprint(latestFingerprint).build()),
                SET_METADATA,
                zone);
    }

    List<Zone> fetchZones() {
        return executeSynchronouslyWithRetries(() -> StreamSupport.stream(zones.list(project).iterateAll().spliterator(), false)
                .filter(zone -> zone.getRegion().endsWith(region))
                .collect(Collectors.toList()), LIST_ZONES);
    }

    private Operation executeSynchronously(final OperationFuture<Operation, Operation> future, final String opName, final String zoneName) {
        try {
            Operation operation = future.get();
            if (operation.getStatus() != Status.DONE) {
                Operation waitForCompletion = zoneOperations.wait(project, zoneName, opName);
                if (waitForCompletion.hasError()) {
                    throw new RuntimeException(waitForCompletion.getError()
                            .getErrorsList()
                            .stream()
                            .map(Errors::getMessage)
                            .collect(Collectors.joining(",")));
                }
            }
            return operation;
        } catch (Exception e) {
            String message = format("Failed synchronous execution of [%s]", opName);
            LOGGER.error(message, e);
            //            LOGGER.info("Request: {}", future.getInitialFuture());
            throw new RuntimeException(message, e);
        }
    }

    private <T> T executeSynchronouslyWithRetries(final CheckedSupplier<T> operationCheckedSupplier, final String opName) {
        return Failsafe.with(new RetryPolicy<>().handle(Exception.class)
                .withDelay(Duration.ofSeconds(pollInterval))
                .withMaxRetries(MAX_RETRIES)
                .onFailedAttempt(rExecutionAttemptedEvent -> LOGGER.error("[{}] failed: {}",
                        opName,
                        rExecutionAttemptedEvent.getLastFailure().getMessage()))).get(operationCheckedSupplier);
    }
}