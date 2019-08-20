package com.hartwig.pipeline.alignment.merge;

import java.util.List;

public interface PerLaneAlignmentOutput {

    List<IndexedBamLocation> bams();
}
