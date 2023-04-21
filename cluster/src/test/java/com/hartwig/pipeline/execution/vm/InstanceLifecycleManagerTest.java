package com.hartwig.pipeline.execution.vm;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.compute.v1.Instance;
import com.google.cloud.compute.v1.InstancesClient;
import com.google.cloud.compute.v1.Operation;
import com.google.cloud.compute.v1.Zone;
import com.google.cloud.compute.v1.ZoneOperationsClient;
import com.google.cloud.compute.v1.ZonesClient;
import com.hartwig.pipeline.Arguments;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Before;
import org.junit.Test;

public class InstanceLifecycleManagerTest {
    private static final Arguments ARGUMENTS = Arguments.testDefaults();

    private String zoneOne;
    private String zoneTwo;
    private String vmName;
    private InstancesClient instances;
    private ZonesClient zones;
    private ZoneOperationsClient zoneOperations;
    private InstanceLifecycleManager victim;

    @Before
    public void setup() {
        zoneOne = "zone-a";
        zoneTwo = "zone-b";
        vmName = "vm-name";
        instances = mock(InstancesClient.class);
        zones = mock(ZonesClient.class);
        zoneOperations = mock(ZoneOperationsClient.class);
        victim = new InstanceLifecycleManager(ARGUMENTS, instances, zones, zoneOperations);
    }

    @Test
    public void shouldReturnExistingInstance() {
        InstancesClient.ListPagedResponse zoneOneInstanceList = mock(InstancesClient.ListPagedResponse.class);
        InstancesClient.ListPagedResponse zoneTwoInstanceList = mock(InstancesClient.ListPagedResponse.class);

        when(instances.list(ARGUMENTS.project(), zoneOne)).thenReturn(zoneOneInstanceList);
        when(instances.list(ARGUMENTS.project(), zoneTwo)).thenReturn(zoneTwoInstanceList);

        Instance vmInstance = namedInstance(vmName);
        Instance someInstance = namedInstance();
        Instance someOtherInstance = namedInstance();
        when(zoneOneInstanceList.iterateAll()).thenReturn(List.of(someInstance));
        when(zoneTwoInstanceList.iterateAll()).thenReturn(List.of(someOtherInstance, vmInstance));

        List<Zone> stubbedZones = List.of(zone(zoneOne), zone(zoneTwo));
        ZonesClient.ListPagedResponse zoneListResponse = mock(ZonesClient.ListPagedResponse.class);
        when(zoneListResponse.iterateAll()).thenReturn(stubbedZones);
        when(zones.list(ARGUMENTS.project())).thenReturn(zoneListResponse);

        Optional<Instance> found = victim.findExistingInstance(vmName);
        assertThat(found).isNotEmpty();
        assertThat(found.get()).isEqualTo(vmInstance);
    }

    @Test
    public void shouldReturnEmptyOptionalIfNamedInstanceDoesNotExist() {
        InstancesClient.ListPagedResponse zoneOneInstanceList = mock(InstancesClient.ListPagedResponse.class);
        InstancesClient.ListPagedResponse zoneTwoInstanceList = mock(InstancesClient.ListPagedResponse.class);

        when(instances.list(ARGUMENTS.project(), zoneOne)).thenReturn(zoneOneInstanceList);
        when(instances.list(ARGUMENTS.project(), zoneTwo)).thenReturn(zoneTwoInstanceList);

        List<Instance> zoneOneInstanceItems = List.of(namedInstance());
        List<Instance> zoneTwoInstanceItems = List.of(namedInstance(), namedInstance());
        when(zoneOneInstanceList.iterateAll()).thenReturn(zoneOneInstanceItems);
        when(zoneTwoInstanceList.iterateAll()).thenReturn(zoneTwoInstanceItems);

        List<Zone> stubbedZones = List.of(zone(zoneOne), zone(zoneTwo));
        ZonesClient.ListPagedResponse zoneListResponse = mock(ZonesClient.ListPagedResponse.class);
        when(zoneListResponse.iterateAll()).thenReturn(stubbedZones);
        when(zones.list(ARGUMENTS.project())).thenReturn(zoneListResponse);

        Optional<Instance> found = victim.findExistingInstance(vmName);
        assertThat(found).isEmpty();
    }

    @Test
    public void shouldDelete() throws Exception {
        Operation deleteOperation = Operation.newBuilder().setName("delete").setStatus(Operation.Status.DONE).build();
        OperationFuture<Operation, Operation> deleteOperationFuture = operationFuture();
        when(deleteOperationFuture.get()).thenReturn(deleteOperation);
        when(instances.deleteAsync(ARGUMENTS.project(), zoneOne, vmName)).thenReturn(deleteOperationFuture);

        when(zoneOperations.get(ARGUMENTS.project(), zoneOne, "delete")).thenReturn(deleteOperation);

        victim.delete(zoneOne, vmName);
    }

    @Test
    public void shouldStop() throws Exception {
        Operation stopOperation = Operation.newBuilder().setName("stop").setStatus(Operation.Status.DONE).build();
        OperationFuture<Operation, Operation> operationFuture = operationFuture();
        when(operationFuture.get()).thenReturn(stopOperation);
        when(instances.stopAsync(ARGUMENTS.project(), zoneOne, vmName)).thenReturn(operationFuture);

        Operation statusOperation = Operation.newBuilder().setName("status").setStatus(Operation.Status.DONE).build();
        when(zoneOperations.get(ARGUMENTS.project(), zoneOne, "stop")).thenReturn(statusOperation);

        victim.stop(zoneOne, vmName);
    }

    private Zone zone(final String name) {
        Zone zone = mock(Zone.class);
        when(zone.getName()).thenReturn(name);
        when(zone.getRegion()).thenReturn("someregion-" + ARGUMENTS.region());
        return zone;
    }

    private Instance namedInstance() {
        return namedInstance(null);
    }

    private Instance namedInstance(final String providedName) {
        @SuppressWarnings("deprecation")
        String name = providedName == null ? RandomStringUtils.random(10) : providedName;
        Instance instance = mock(Instance.class);
        when(instance.getName()).thenReturn(name);
        return instance;
    }

    @SuppressWarnings("unchecked")
    private OperationFuture<Operation, Operation> operationFuture() {
        return mock(OperationFuture.class);
    }
}