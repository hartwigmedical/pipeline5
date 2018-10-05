package com.hartwig.pipeline.cost;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.api.services.cloudbilling.model.Sku;

public interface Costs {

    List<Cost> list(Map<String, Sku> skus);

    static Costs defaultCosts() {
        return skus -> Arrays.asList(ResourceCost.masterCpu(skus),
                ResourceCost.masterMemory(skus),
                ResourceCost.primaryCpu(skus),
                ResourceCost.primaryMemory(skus),
                ResourceCost.preemptibleCpu(skus),
                ResourceCost.preemtibleMemory(skus),
                DataprocCost.create());
    }
}
