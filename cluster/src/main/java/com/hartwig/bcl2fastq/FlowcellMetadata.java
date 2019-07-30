package com.hartwig.bcl2fastq;

import org.immutables.value.Value;

@Value.Immutable
public interface FlowcellMetadata {

    @Value.Parameter
    String flowcellId();

    static FlowcellMetadata from (Arguments arguments){
        return ImmutableFlowcellMetadata.of(arguments.flowcell());
    }
}
