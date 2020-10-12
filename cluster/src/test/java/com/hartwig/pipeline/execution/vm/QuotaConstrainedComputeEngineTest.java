package com.hartwig.pipeline.execution.vm;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

import com.google.api.services.serviceusage.v1beta1.ServiceUsage;
import com.google.api.services.serviceusage.v1beta1.model.ConsumerQuotaLimit;
import com.google.api.services.serviceusage.v1beta1.model.QuotaBucket;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.execution.MachineType;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.testsupport.MockRuntimeBucket;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class QuotaConstrainedComputeEngineTest {

    private static final String REGION = "region";
    private static final String PROJECT = "project";

    @Test
    public void ensuresVmsDontExceedMaxCPUForRegion() throws Exception {
        ComputeEngine decorated = mock(ComputeEngine.class);
        ServiceUsage serviceUsage = mock(ServiceUsage.class);
        ServiceUsage.Services services = mock(ServiceUsage.Services.class);
        ServiceUsage.Services.ConsumerQuotaMetrics consumerQuotaMetrics = mock(ServiceUsage.Services.ConsumerQuotaMetrics.class);
        ServiceUsage.Services.ConsumerQuotaMetrics.Limits limits = mock(ServiceUsage.Services.ConsumerQuotaMetrics.Limits.class);
        ServiceUsage.Services.ConsumerQuotaMetrics.Limits.Get limitsGet = mock(ServiceUsage.Services.ConsumerQuotaMetrics.Limits.Get.class);

        when(serviceUsage.services()).thenReturn(services);
        when(services.consumerQuotaMetrics()).thenReturn(consumerQuotaMetrics);
        when(consumerQuotaMetrics.limits()).thenReturn(limits);
        ArgumentCaptor<String> quotaName = ArgumentCaptor.forClass(String.class);
        when(limits.get(quotaName.capture())).thenReturn(limitsGet);
        ConsumerQuotaLimit limit = new ConsumerQuotaLimit().setQuotaBuckets(List.of(new QuotaBucket().setEffectiveLimit(10L)
                .setDimensions(Map.of(REGION, REGION))));
        when(limitsGet.execute()).thenReturn(limit);

        VirtualMachineJobDefinition jobDefinition = VirtualMachineJobDefinition.builder()
                .name("test")
                .namespacedResults(ResultsDirectory.defaultDirectory())
                .startupCommand(BashStartupScript.of("empty"))
                .performanceProfile(VirtualMachinePerformanceProfile.custom(10, 10))
                .build();

        ArgumentCaptor<VirtualMachineJobDefinition> constrained = ArgumentCaptor.forClass(VirtualMachineJobDefinition.class);
        when(decorated.submit(any(), constrained.capture(), any())).thenReturn(PipelineStatus.SUCCESS);

        QuotaConstrainedComputeEngine victim = new QuotaConstrainedComputeEngine(decorated, serviceUsage, REGION, PROJECT, 0.6);
        PipelineStatus result = victim.submit(MockRuntimeBucket.test().getRuntimeBucket(), jobDefinition);
        assertThat(result).isEqualTo(PipelineStatus.SUCCESS);
        MachineType machineType = constrained.getValue().performanceProfile().machineType();
        assertThat(machineType.cpus()).isEqualTo(6);
        assertThat(machineType.memoryGB()).isEqualTo(6);
    }

}