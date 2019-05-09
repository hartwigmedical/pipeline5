package com.hartwig.pipeline;

import com.hartwig.pipeline.execution.JobStatus;

public interface StageOutput {

    String name();

    JobStatus status();
}
