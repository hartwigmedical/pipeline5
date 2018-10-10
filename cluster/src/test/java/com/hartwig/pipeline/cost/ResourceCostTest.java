package com.hartwig.pipeline.cost;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;

import com.google.api.services.cloudbilling.model.Money;
import com.google.api.services.cloudbilling.model.PricingExpression;
import com.google.api.services.cloudbilling.model.PricingInfo;
import com.google.api.services.cloudbilling.model.Sku;
import com.google.api.services.cloudbilling.model.TierRate;
import com.google.common.collect.ImmutableMap;
import com.hartwig.pipeline.performance.ImmutableMachineType;
import com.hartwig.pipeline.performance.ImmutablePerformanceProfile;
import com.hartwig.pipeline.performance.PerformanceProfile;

import org.assertj.core.data.Offset;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

public class ResourceCostTest {

    @Test
    public void calculatesMasterCpuResourceCost() {
        Sku sku = skuWithSingleTierRate(0.034);
        ResourceCost victim = ResourceCost.masterCpu(ImmutableMap.of(ResourceCost.CPU_CORE_SKU, sku));
        double cost = victim.calculate(profileWith().master(machineWith().cpus(10).build()).build(), 2);
        assertThat(cost).isEqualTo(0.68, Offset.offset(0.001));
    }

    @Test
    public void calculatesMasterMemoryResourceCost() {
        Sku sku = skuWithSingleTierRate(0.0046);
        ResourceCost victim = ResourceCost.masterMemory(ImmutableMap.of(ResourceCost.RAM_GB_SKU, sku));
        double cost = victim.calculate(profileWith().master(machineWith().memoryGB(10).build()).build(), 2);
        assertThat(cost).isEqualTo(0.092);
    }

    @Test
    public void calculatesPrimaryCpuResourceCost() {
        Sku sku = skuWithSingleTierRate(0.034);
        ResourceCost victim = ResourceCost.primaryCpu(ImmutableMap.of(ResourceCost.CPU_CORE_SKU, sku));
        double cost = victim.calculate(profileWith().numPrimaryWorkers(2).primaryWorkers(machineWith().cpus(10).build()).build(), 2);
        assertThat(cost).isEqualTo(1.36, Offset.offset(0.001));
    }

    @Test
    public void calculatesPrimaryMemoryResourceCost() {
        Sku sku = skuWithSingleTierRate(0.0046);
        ResourceCost victim = ResourceCost.primaryMemory(ImmutableMap.of(ResourceCost.RAM_GB_SKU, sku));
        double cost = victim.calculate(profileWith().numPrimaryWorkers(2).primaryWorkers(machineWith().memoryGB(10).build()).build(), 2);
        assertThat(cost).isEqualTo(.184);
    }

    @Test
    public void calculatesPreemptibleCpuResourceCost() {
        Sku sku = skuWithSingleTierRate(0.007);
        ResourceCost victim = ResourceCost.preemptibleCpu(ImmutableMap.of(ResourceCost.CPU_CORE_PREEMPTIBLE_SKU, sku));
        double cost = victim.calculate(profileWith().numPreemtibleWorkers(2).preemtibleWorkers(machineWith().cpus(10).build()).build(), 2);
        assertThat(cost).isEqualTo(0.28, Offset.offset(0.001));
    }

    @Test
    public void calculatesPreemptibleMemoryResourceCost() {
        Sku sku = skuWithSingleTierRate(0.001);
        ResourceCost victim = ResourceCost.preemtibleMemory(ImmutableMap.of(ResourceCost.RAM_GB_PREEMPTIBLE_SKU, sku));
        double cost =
                victim.calculate(profileWith().numPreemtibleWorkers(2).preemtibleWorkers(machineWith().memoryGB(10).build()).build(), 2);
        assertThat(cost).isEqualTo(.04);
    }

    @NotNull
    private ImmutablePerformanceProfile.Builder profileWith() {
        return PerformanceProfile.builder().numPrimaryWorkers(1).numPreemtibleWorkers(1);
    }

    @NotNull
    private Sku skuWithSingleTierRate(final double rateInDollars) {
        int rateInNanoCents = (int) (rateInDollars * 1e9);
        return new Sku().setPricingInfo(Collections.singletonList(new PricingInfo().setPricingExpression(new PricingExpression().setTieredRates(
                Collections.singletonList(new TierRate().setUnitPrice(new Money().setNanos(rateInNanoCents)))))));
    }

    private ImmutableMachineType.Builder machineWith() {
        return ImmutableMachineType.builder().costPerInstancePerHour(0).diskGB(0).cpus(0).memoryGB(0).uri("");
    }
}