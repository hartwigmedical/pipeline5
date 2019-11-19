package com.hartwig.bcl2fastq;

import com.hartwig.pipeline.metadata.RunMetadata;

import org.immutables.value.Value;

@Value.Immutable
public interface FlowcellMetadata extends RunMetadata {

    @Value.Parameter
    String flowcellId();

    @Override
    default String name() {
        return flowcellId();
    }

    static FlowcellMetadata from(Arguments arguments) {
        return ImmutableFlowcellMetadata.of(arguments.flowcell());
    }
}
