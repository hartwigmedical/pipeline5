package com.hartwig.pipeline.calling.structural;

import com.hartwig.pipeline.execution.JobStatus;
import com.hartwig.pipeline.io.GoogleStorageLocation;

public interface StructuralCallerOutput {

    JobStatus status();

    GoogleStorageLocation structuralVcf();

    GoogleStorageLocation svRecoveryVcf();
}
