package com.hartwig.pipeline.execution.vm;

import static java.lang.String.format;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.api.services.compute.Compute;
import com.google.api.services.compute.ComputeRequest;
import com.google.api.services.compute.model.Instance;
import com.google.api.services.compute.model.InstanceList;
import com.google.api.services.compute.model.Metadata;
import com.google.api.services.compute.model.Operation;
import com.google.api.services.compute.model.Zone;
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
    private final Compute compute;
    private final String region;
    private final Integer pollInterval;

    InstanceLifecycleManager(final CommonArguments arguments, final Compute compute) {
        this.project = arguments.project();
        this.region = arguments.region();
        this.pollInterval = arguments.pollInterval();
        this.compute = compute;
    }

    Instance newInstance() {
        return new Instance();
    }

    Optional<Instance> findExistingInstance(String vmName) {
        for (String zone : fetchZones().stream().map(Zone::getName).collect(Collectors.toList())) {
            InstanceList instances = executeWithRetries(() -> compute.instances().list(project, zone).execute(), LIST_INSTANCES);
            if (instances.getItems() != null) {
                for (Instance instance : instances.getItems()) {
                    if (instance.getName().equals(vmName)) {
                        return Optional.of(instance);
                    }
                }
            }
        }
        return Optional.empty();
    }

    Operation deleteOldInstancesAndStart(Instance instance, String zone, String vmName) {
        findExistingInstance(vmName).ifPresent(i -> {
            try {
                String shortZone = new File(i.getZone()).getName();
                LOGGER.debug("Removing existing VM instance [{}] in [{}]", i.getName(), shortZone);
                Operation delete =
                        executeSynchronously(compute.instances().delete(project, shortZone, vmName), project, shortZone, DELETE_VM);
                if (delete.getError() != null) {
                    throw new RuntimeException(delete.getError().toPrettyString());
                }
            } catch (Exception e) {
                throw new RuntimeException("Could not delete existing [" + vmName + "] instance", e);
            }
        });
        try {
            return executeSynchronously(compute.instances().insert(project, zone, instance), project, zone, "insertVm");
        } catch (IOException ioe) {
            throw new RuntimeException("Could not initialise insert operation!", ioe);
        }
    }

    void delete(String zone, String vm) {
        executeSynchronously(getWithRetries(() -> compute.instances().delete(project, zone, vm)), project, zone, DELETE_VM);
    }

    void stop(String zone, String vm) {
        executeSynchronously(getWithRetries(() -> compute.instances().stop(project, zone, vm)), project, zone, STOP_VM);
    }

    private String operationStatus(String jobName, String zoneName) {
        return executeWithRetries(() -> compute.zoneOperations().get(project, zoneName, jobName).execute(), OPERATION_STATUS).getStatus();
    }

    String instanceStatus(String vm, String zone) {
        try {
            Instance found = compute.instances().get(project, zone, vm).execute();
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
        String latestFingerprint = compute.instances().get(project, zone, vm).execute().getMetadata().getFingerprint();
        executeSynchronously(compute.instances().setMetadata(project, zone, vm, new Metadata().setFingerprint(latestFingerprint)),
                project,
                zone, SET_METADATA);
    }

    private ComputeRequest<Operation> getWithRetries(final CheckedSupplier<ComputeRequest<Operation>> supplier) {
        return Failsafe.with(new RetryPolicy<>().handle(Exception.class).withDelay(Duration.ofSeconds(pollInterval)).withMaxRetries(5))
                .get(supplier);
    }

    private Operation executeSynchronously(ComputeRequest<Operation> request, String projectName, String zoneName, String opName) {
        Operation asyncOp = executeWithRetries(request::execute, opName);
        String logId = format("Operation [%s:%s]", asyncOp.getOperationType(), asyncOp.getName());
        LOGGER.debug("{} is executing synchronously", logId);
        while (RUNNING_STATUS.equals(operationStatus(asyncOp.getName(), zoneName))) {
            LOGGER.debug("{} not done yet", logId);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }
        return executeWithRetries(() -> compute.zoneOperations().get(projectName, zoneName, asyncOp.getName()).execute(),
                GET_ZONE_OPERATIONS);
    }

    private <T> T executeWithRetries(final CheckedSupplier<T> operationCheckedSupplier, final String opName) {
        return Failsafe.with(DefaultBackoffPolicy.of(String.format("Lifecycly manager operation [%s]", opName)))
                .get(operationCheckedSupplier);
    }

    private List<Zone> fetchZones() {
        return executeWithRetries(() -> compute.zones()
                .list(project)
                .execute()
                .getItems()
                .stream()
                .filter(zone -> zone.getRegion().endsWith(region))
                .collect(Collectors.toList()), LIST_ZONES);
    }
}
