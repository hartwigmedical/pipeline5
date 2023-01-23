package com.hartwig.pipeline.execution.vm;

import static java.lang.String.format;

import java.io.File;
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
import com.google.cloud.compute.v1.Zone;
import com.google.cloud.compute.v1.ZoneOperationsClient;
import com.google.cloud.compute.v1.ZonesClient;
import com.hartwig.pipeline.CommonArguments;
import com.hartwig.pipeline.failsafe.DefaultBackoffPolicy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import net.jodah.failsafe.function.CheckedSupplier;

class InstanceLifecycleManager {
    private static final String RUNNING_STATUS = "RUNNING";
    private static final Logger LOGGER = LoggerFactory.getLogger(InstanceLifecycleManager.class);
    private static final String LIST_INSTANCES = "listInstances";
    public static final String DELETE_VM = "deleteVm";
    public static final String STOP_VM = "stopVm";
    public static final String OPERATION_STATUS = "operationStatus";
    public static final String SET_METADATA = "setMetadata";
    public static final String GET_ZONE_OPERATIONS = "getZoneOperations";
    public static final String LIST_ZONES = "listZones";

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
            Iterable<Instance> instances = executeWithRetries(() -> this.instances.list(project, zone).iterateAll(), LIST_INSTANCES);
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

    Operation deleteOldInstancesAndStart(final Instance instance, final String zone, final String vmName) {
        findExistingInstance(vmName).ifPresent(i -> {
            try {
                String shortZone = new File(i.getZone()).getName();
                LOGGER.debug("Removing existing VM instance [{}] in [{}]", i.getName(), shortZone);
                Operation delete = executeSynchronously(instances.deleteAsync(project, shortZone, vmName), project, shortZone, DELETE_VM);
                if (!delete.getError().getErrorsList().isEmpty()) {
                    throw new RuntimeException(delete.getError()
                            .getErrorsList()
                            .stream()
                            .map(Errors::getMessage)
                            .collect(Collectors.joining(",")));
                }
            } catch (Exception e) {
                throw new RuntimeException("Could not delete existing [" + vmName + "] instance", e);
            }
        });
        return executeSynchronously(instances.insertAsync(project, zone, instance), project, zone, "insertVm");
    }

    void delete(final String zone, final String vm) {
        executeSynchronously(getWithRetries(() -> instances.deleteAsync(project, zone, vm)), project, zone, DELETE_VM);
    }

    void stop(final String zone, final String vm) {
        executeSynchronously(getWithRetries(() -> instances.stopAsync(project, zone, vm)), project, zone, STOP_VM);
    }

    private String operationStatus(final String jobName, final String zoneName) {
        return executeWithRetries(() -> zoneOperations.get(project, zoneName, jobName), OPERATION_STATUS).getStatus()
                .getValueDescriptor()
                .getName();
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

    void disableStartupScript(final String zone, final String vm) throws IOException {
        String latestFingerprint = instances.get(project, zone, vm).getMetadata().getFingerprint();
        executeSynchronously(instances.setMetadataAsync(project, zone, vm, Metadata.newBuilder().setFingerprint(latestFingerprint).build()),
                project,
                zone,
                SET_METADATA);
    }

    private OperationFuture<Operation, Operation> getWithRetries(final CheckedSupplier<OperationFuture<Operation, Operation>> supplier) {
        return Failsafe.with(new RetryPolicy<>().handle(Exception.class).withDelay(Duration.ofSeconds(pollInterval)).withMaxRetries(5))
                .get(supplier);
    }

    private Operation executeSynchronously(final OperationFuture<Operation, Operation> request, final String projectName,
            final String zoneName, final String opName) {
        Operation asyncOp = executeWithRetries(request::get, opName);
        String logId = format("Operation [%s:%s]", asyncOp.getOperationType(), asyncOp.getName());
        LOGGER.debug("{} is executing synchronously", logId);
        while (RUNNING_STATUS.equals(operationStatus(asyncOp.getName(), zoneName))) {
            LOGGER.debug("{} not done yet", logId);
            try {
                //noinspection BusyWait
                Thread.sleep(1000);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }
        return executeWithRetries(() -> zoneOperations.get(projectName, zoneName, asyncOp.getName()), GET_ZONE_OPERATIONS);
    }

    private <T> T executeWithRetries(final CheckedSupplier<T> operationCheckedSupplier, final String opName) {
        return Failsafe.with(DefaultBackoffPolicy.of(String.format("Lifecycle manager operation [%s]", opName)))
                .get(operationCheckedSupplier);
    }

    private List<Zone> fetchZones() {
        return executeWithRetries(() -> StreamSupport.stream(zones.list(project).iterateAll().spliterator(), false)
                .filter(zone -> zone.getRegion().endsWith(region))
                .collect(Collectors.toList()), LIST_ZONES);
    }
}