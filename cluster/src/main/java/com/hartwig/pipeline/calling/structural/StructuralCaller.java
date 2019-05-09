package com.hartwig.pipeline.calling.structural;

import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.bammetrics.BamMetricsOutput;
import com.hartwig.pipeline.execution.JobStatus;

public class StructuralCaller {

    public StructuralCallerOutput run(AlignmentPair pair, BamMetricsOutput metricsOutput){
        return StructuralCallerOutput.builder().status(JobStatus.SKIPPED).build();
    }
}
