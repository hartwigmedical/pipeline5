package com.hartwig.pipeline.alignment.merge;

import java.util.List;

import org.immutables.value.Value;

@Value.Immutable
public interface PerLaneAlignmentOutput {

    List<IndexedBamLocation> bams();

    static ImmutablePerLaneAlignmentOutput.Builder builder (){
        return ImmutablePerLaneAlignmentOutput.builder();
    }
}
