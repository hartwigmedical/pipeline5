package com.hartwig.pipeline.cost;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Map;
import java.util.function.Function;

import com.google.api.services.cloudbilling.model.Sku;
import com.hartwig.pipeline.execution.dataproc.DataprocPerformanceProfile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ResourceCost implements Cost {

    static final String CPU_CORE_SKU = "7EFD-BE8D-9E97";
    static final String CPU_CORE_PREEMPTIBLE_SKU = "2117-21EA-205A";
    static final String RAM_GB_SKU = "17DC-172A-BC1B";
    static final String RAM_GB_PREEMPTIBLE_SKU = "9D71-ECE5-BF3F";
    private static final String PRIMARY_WORKER = "Primary";
    private static final String MEMORY_GB = "Memory GB";
    private static final String CPU = "CPU";
    private static final String MASTER = "Master";
    private static final String PREEMTIBLE_WORKER = "Preemtible";
    private final Logger LOGGER = LoggerFactory.getLogger(ResourceCost.class);
    private final Sku sku;
    private final Function<DataprocPerformanceProfile, Integer> resourceCountFunction;
    private final Function<DataprocPerformanceProfile, Integer> numMachineFunction;
    private final String resourceGroup;
    private final String resourceName;

    private ResourceCost(final Sku sku, final Function<DataprocPerformanceProfile, Integer> resourceCountFunction,
            final Function<DataprocPerformanceProfile, Integer> numMachineFunction, final String resourceGroup, final String resourceName) {
        this.sku = sku;
        this.resourceCountFunction = resourceCountFunction;
        this.numMachineFunction = numMachineFunction;
        this.resourceGroup = resourceGroup;
        this.resourceName = resourceName;
    }

    public double calculate(DataprocPerformanceProfile performanceProfile, double hours) {
        int numMachines = numMachineFunction.apply(performanceProfile);
        int totalUnits = resourceCountFunction.apply(performanceProfile) * numMachines;
        double rate = priceInDollars(sku);
        double resourceCost = hours * rate * totalUnits;
        LOGGER.debug("[{} {}] instances using a total defaultDirectory [{} {}] for [{}] hours at [{}] per hour/per {} for a total cost defaultDirectory [{}]",
                numMachines,
                resourceName,
                totalUnits,
                resourceGroup,
                hours,
                DecimalFormat.getNumberInstance().format(rate),
                resourceName,
                NumberFormat.getCurrencyInstance().format(resourceCost));
        return resourceCost;
    }

    private double priceInDollars(final Sku sku) {
        return sku.getPricingInfo().get(0).getPricingExpression().getTieredRates().get(0).getUnitPrice().getNanos() / 1e9;
    }

    static ResourceCost masterCpu(Map<String, Sku> skus) {
        return new ResourceCost(skus.get(CPU_CORE_SKU),
                performanceProfile -> performanceProfile.master().cpus(),
                performanceProfile -> 1,
                CPU,
                MASTER);
    }

    static ResourceCost masterMemory(Map<String, Sku> skus) {
        return new ResourceCost(skus.get(RAM_GB_SKU),
                performanceProfile -> performanceProfile.master().memoryGB(),
                performanceProfile -> 1,
                MEMORY_GB,
                MASTER);
    }

    static ResourceCost primaryCpu(Map<String, Sku> skus) {
        return new ResourceCost(skus.get(CPU_CORE_SKU),
                performanceProfile -> performanceProfile.primaryWorkers().cpus(),
                DataprocPerformanceProfile::numPrimaryWorkers,
                CPU,
                PRIMARY_WORKER);
    }

    static ResourceCost primaryMemory(Map<String, Sku> skus) {
        return new ResourceCost(skus.get(RAM_GB_SKU),
                performanceProfile -> performanceProfile.primaryWorkers().memoryGB(),
                DataprocPerformanceProfile::numPrimaryWorkers,
                MEMORY_GB,
                PRIMARY_WORKER);
    }

    static ResourceCost preemptibleCpu(Map<String, Sku> skus) {
        return new ResourceCost(skus.get(CPU_CORE_PREEMPTIBLE_SKU),
                performanceProfile -> performanceProfile.preemtibleWorkers().cpus(),
                DataprocPerformanceProfile::numPreemtibleWorkers,
                CPU,
                PREEMTIBLE_WORKER);
    }

    static ResourceCost preemtibleMemory(Map<String, Sku> skus) {
        return new ResourceCost(skus.get(RAM_GB_PREEMPTIBLE_SKU),
                performanceProfile -> performanceProfile.preemtibleWorkers().memoryGB(),
                DataprocPerformanceProfile::numPreemtibleWorkers,
                MEMORY_GB,
                PREEMTIBLE_WORKER);
    }
}
