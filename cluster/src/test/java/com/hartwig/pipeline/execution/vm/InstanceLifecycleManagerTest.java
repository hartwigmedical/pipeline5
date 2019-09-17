package com.hartwig.pipeline.execution.vm;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

import static com.hartwig.pipeline.SomaticPipelineTest.ARGUMENTS;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import com.google.api.services.compute.Compute;
import com.google.api.services.compute.model.Instance;
import com.google.api.services.compute.model.InstanceList;
import com.google.api.services.compute.model.Operation;
import com.google.api.services.compute.model.Zone;
import com.google.api.services.compute.model.ZoneList;

import org.apache.commons.lang.RandomStringUtils;
import org.junit.Before;
import org.junit.Test;

public class InstanceLifecycleManagerTest {
    private static final String DONE_STATUS = "DONE";

    private String zoneOne;
    private String zoneTwo;
    private String vmName;
    private Compute compute;
    private Compute.Instances instances;
    private InstanceLifecycleManager victim;

    @Before
    public void setup() {
        zoneOne = "zone-a";
        zoneTwo = "zone-b";
        vmName = "vm-name";
        compute = mock(Compute.class);
        instances = mock(Compute.Instances.class);
        victim = new InstanceLifecycleManager(ARGUMENTS, compute);

        when(compute.instances()).thenReturn(instances);
    }

    @Test
    public void shouldReturnExistingInstance() throws Exception {
        Compute.Instances.List zoneOneInstances = mock(Compute.Instances.List.class);
        Compute.Instances.List zoneTwoInstances = mock(Compute.Instances.List.class);

        InstanceList zoneOneInstanceList = mock(InstanceList.class);
        InstanceList zoneTwoInstanceList = mock(InstanceList.class);

        when(instances.list(ARGUMENTS.project(), zoneOne)).thenReturn(zoneOneInstances);
        when(instances.list(ARGUMENTS.project(), zoneTwo)).thenReturn(zoneTwoInstances);

        when(zoneOneInstances.execute()).thenReturn(zoneOneInstanceList);
        when(zoneTwoInstances.execute()).thenReturn(zoneTwoInstanceList);

        Instance vmInstance = namedInstance(vmName);
        Instance someInstance = namedInstance();
        Instance someOtherInstance = namedInstance();
        when(zoneOneInstanceList.getItems()).thenReturn(singletonList(someInstance));
        when(zoneTwoInstanceList.getItems()).thenReturn(asList(someOtherInstance, vmInstance));

        Compute.Zones zones = mock(Compute.Zones.class);
        Compute.Zones.List zonesList = mock(Compute.Zones.List.class);
        List<Zone> stubbedZones = asList(zone(zoneOne), zone(zoneTwo));
        when(zonesList.execute()).thenReturn(new ZoneList().setItems(stubbedZones));
        when(compute.zones()).thenReturn(zones);
        when(zones.list(ARGUMENTS.project())).thenReturn(zonesList);

        Optional<Instance> found = victim.findExistingInstance(vmName);
        assertThat(found).isNotEmpty();
        assertThat(found.get()).isEqualTo(vmInstance);
    }

    @Test
    public void shouldReturnEmptyOptionalIfNamedInstanceDoesNotExist() throws Exception {
        Compute.Instances.List zoneOneInstances = mock(Compute.Instances.List.class);
        Compute.Instances.List zoneTwoInstances = mock(Compute.Instances.List.class);

        InstanceList zoneOneInstanceList = mock(InstanceList.class);
        InstanceList zoneTwoInstanceList = mock(InstanceList.class);

        when(instances.list(ARGUMENTS.project(), zoneOne)).thenReturn(zoneOneInstances);
        when(instances.list(ARGUMENTS.project(), zoneTwo)).thenReturn(zoneTwoInstances);

        when(zoneOneInstances.execute()).thenReturn(zoneOneInstanceList);
        when(zoneTwoInstances.execute()).thenReturn(zoneTwoInstanceList);

        List<Instance> zoneOneInstanceItems = singletonList(namedInstance());
        List<Instance> zoneTwoInstanceItems = asList(namedInstance(), namedInstance());
        when(zoneOneInstanceList.getItems()).thenReturn(zoneOneInstanceItems);
        when(zoneTwoInstanceList.getItems()).thenReturn(zoneTwoInstanceItems);

        Compute.Zones zones = mock(Compute.Zones.class);
        Compute.Zones.List zonesList = mock(Compute.Zones.List.class);
        List<Zone> stubbedZones = asList(zone(zoneOne), zone(zoneTwo));
        when(zonesList.execute()).thenReturn(new ZoneList().setItems(stubbedZones));
        when(compute.zones()).thenReturn(zones);
        when(zones.list(ARGUMENTS.project())).thenReturn(zonesList);

        Optional<Instance> found = victim.findExistingInstance(vmName);
        assertThat(found).isEmpty();
    }

    @Test
    public void shouldDelete() throws Exception {
        Compute.Instances.Delete delete = mock(Compute.Instances.Delete.class);
        Operation deleteOperation = mock(Operation.class);
        String deleteOperationName = "delete";

        when(instances.delete(ARGUMENTS.project(), zoneOne, vmName)).thenReturn(delete);
        when(delete.execute()).thenReturn(deleteOperation);
        when(deleteOperation.getName()).thenReturn(deleteOperationName);
        when(deleteOperation.getStatus()).thenReturn(DONE_STATUS);

        Compute.ZoneOperations zoneOperations = mock(Compute.ZoneOperations.class);
        Compute.ZoneOperations.Get zoneOpsGet = mock(Compute.ZoneOperations.Get.class);
        Operation statusOperation = mock(Operation.class);
        when(compute.zoneOperations()).thenReturn(zoneOperations);
        when(zoneOperations.get(ARGUMENTS.project(), zoneOne, deleteOperationName)).thenReturn(zoneOpsGet);
        when(zoneOpsGet.execute()).thenReturn(statusOperation);
        when(statusOperation.getStatus()).thenReturn(DONE_STATUS);

        victim.delete(zoneOne, vmName);
        verify(delete).execute();
    }

    @Test
    public void shouldRetryDeleteIfNotAtFirstSuccessful() throws IOException {
        Compute.Instances.Delete delete = mock(Compute.Instances.Delete.class);
        Operation deleteOperation = mock(Operation.class);
        String deleteOperationName = "delete";

        when(instances.delete(ARGUMENTS.project(), zoneOne, vmName)).thenThrow(new IOException()).thenReturn(delete);
        when(delete.execute()).thenReturn(deleteOperation);
        when(deleteOperation.getName()).thenReturn(deleteOperationName);
        when(deleteOperation.getStatus()).thenReturn(DONE_STATUS);

        Compute.ZoneOperations zoneOperations = mock(Compute.ZoneOperations.class);
        Compute.ZoneOperations.Get zoneOpsGet = mock(Compute.ZoneOperations.Get.class);
        Operation statusOperation = mock(Operation.class);
        when(compute.zoneOperations()).thenReturn(zoneOperations);
        when(zoneOperations.get(ARGUMENTS.project(), zoneOne, deleteOperationName)).thenReturn(zoneOpsGet);
        when(zoneOpsGet.execute()).thenReturn(statusOperation);
        when(statusOperation.getStatus()).thenReturn(DONE_STATUS);

        victim.delete(zoneOne, vmName);
        verify(delete).execute();
    }

    @Test
    public void shouldStop() throws Exception {
        Compute.Instances.Stop stop = mock(Compute.Instances.Stop.class);
        Operation stopOperation = mock(Operation.class);
        String stopOperationName = "stoperation";

        when(instances.stop(ARGUMENTS.project(), zoneOne, vmName)).thenReturn(stop);
        when(stop.execute()).thenReturn(stopOperation);
        when(stopOperation.getName()).thenReturn(stopOperationName);
        when(stopOperation.getStatus()).thenReturn(DONE_STATUS);

        Compute.ZoneOperations zoneOperations = mock(Compute.ZoneOperations.class);
        Compute.ZoneOperations.Get zoneOpsGet = mock(Compute.ZoneOperations.Get.class);
        Operation statusOperation = mock(Operation.class);
        when(compute.zoneOperations()).thenReturn(zoneOperations);
        when(zoneOperations.get(ARGUMENTS.project(), zoneOne, stopOperationName)).thenReturn(zoneOpsGet);
        when(zoneOpsGet.execute()).thenReturn(statusOperation);
        when(statusOperation.getStatus()).thenReturn(DONE_STATUS);

        victim.stop(zoneOne, vmName);
        verify(stop).execute();
    }

    @Test
    public void shouldRetryStopIfNotAtFirstSuccessful() throws Exception {
        Compute.Instances.Stop stop = mock(Compute.Instances.Stop.class);
        Operation stopOperation = mock(Operation.class);
        String stopOperationName = "stoperation";

        when(instances.stop(ARGUMENTS.project(), zoneOne, vmName)).thenThrow(new IOException()).thenReturn(stop);
        when(stop.execute()).thenReturn(stopOperation);
        when(stopOperation.getName()).thenReturn(stopOperationName);
        when(stopOperation.getStatus()).thenReturn(DONE_STATUS);

        Compute.ZoneOperations zoneOperations = mock(Compute.ZoneOperations.class);
        Compute.ZoneOperations.Get zoneOpsGet = mock(Compute.ZoneOperations.Get.class);
        Operation statusOperation = mock(Operation.class);
        when(compute.zoneOperations()).thenReturn(zoneOperations);
        when(zoneOperations.get(ARGUMENTS.project(), zoneOne, stopOperationName)).thenReturn(zoneOpsGet);
        when(zoneOpsGet.execute()).thenReturn(statusOperation);
        when(statusOperation.getStatus()).thenReturn(DONE_STATUS);

        victim.stop(zoneOne, vmName);
        verify(stop).execute();
    }

    private Zone zone(String name) {
        Zone zone = mock(Zone.class);
        when(zone.getName()).thenReturn(name);
        when(zone.getRegion()).thenReturn("someregion-" + ARGUMENTS.region());
        return zone;
    }

    private Instance namedInstance() {
        return namedInstance(null);
    }

    private Instance namedInstance(String providedName) {
        String name = providedName == null ? RandomStringUtils.random(10) : providedName;
        Instance instance = mock(Instance.class);
        when(instance.getName()).thenReturn(name);
        return instance;
    }
}