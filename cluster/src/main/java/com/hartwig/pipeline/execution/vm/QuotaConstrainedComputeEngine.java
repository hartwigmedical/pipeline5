package com.hartwig.pipeline.execution.vm;

import java.io.IOException;

import com.google.api.services.serviceusage.v1beta1.ServiceUsage;
import com.google.api.services.serviceusage.v1beta1.model.QuotaBucket;
import com.hartwig.pipeline.execution.MachineType;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.storage.RuntimeBucket;

public class QuotaConstrainedComputeEngine implements ComputeEngine {

    private final ComputeEngine decorated;
    private final ServiceUsage serviceUsage;
    private final String project;
    private final String region;
    private final double constrainByPercentage;

    public QuotaConstrainedComputeEngine(final ComputeEngine decorated, final ServiceUsage serviceUsage, final String region,
            final String project, final double constrainByPercentage) {
        this.decorated = decorated;
        this.serviceUsage = serviceUsage;
        this.project = project;
        this.region = region;
        this.constrainByPercentage = constrainByPercentage;
    }

    @Override
    public PipelineStatus submit(final RuntimeBucket bucket, final VirtualMachineJobDefinition jobDefinition) {
        return submit(bucket, jobDefinition, "");
    }

    @Override
    public PipelineStatus submit(final RuntimeBucket bucket, final VirtualMachineJobDefinition jobDefinition, final String discriminator) {
        VirtualMachineJobDefinition constrained = jobDefinition;
        MachineType machineType = jobDefinition.performanceProfile().machineType();
        try {
            QuotaBucket regionalQuota = serviceUsage.services()
                    .consumerQuotaMetrics()
                    .limits()
                    .get(cpuQuotaName(project))
                    .execute()
                    .getQuotaBuckets()
                    .stream()
                    .filter(b -> b.getDimensions() != null)
                    .filter(b -> region(b) != null)
                    .filter(b -> region(b).equals(region))
                    .findFirst()
                    .orElseThrow();
            int maxCPU = (int) (regionalQuota.getEffectiveLimit().intValue() * constrainByPercentage);
            if (machineType.cpus() > maxCPU) {
                double reductionRatio = (double) maxCPU / machineType.cpus();
                constrained = VirtualMachineJobDefinition.builder()
                        .from(constrained)
                        .performanceProfile(VirtualMachinePerformanceProfile.custom(maxCPU,
                                (int) (machineType.memoryGB() * reductionRatio)))
                        .build();
            }
            return decorated.submit(bucket, constrained, discriminator);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String cpuQuotaName(final String project) {
        return "projects/" + project + "/services/compute.googleapis.com/consumerQuotaMetrics/compute.googleapis.com%2Fcpus/"
                + "limits/%2Fproject%2Fregion";
    }

    public String region(final QuotaBucket b) {
        return b.getDimensions().get("region");
    }
}
