package com.hartwig.pipeline.output;

import java.util.List;
import java.util.stream.Collectors;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.pipeline.PipelineState;
import com.hartwig.pipeline.PipelineStatus;
import com.hartwig.pipeline.StageOutput;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VmExecutionLogSummary {

    private static final Logger LOGGER = LoggerFactory.getLogger(VmExecutionLogSummary.class);

    public static void ofFailedStages(final Storage storage, final PipelineState state) {
        List<StageOutput> failures = state.stageOutputs()
                .stream()
                .filter(stageOutput -> stageOutput.status().equals(PipelineStatus.FAILED))
                .collect(Collectors.toList());
        if (!failures.isEmpty()) {
            LOGGER.error("Failures in pipeline stages. Printing each failures full run.log here");
            for (StageOutput failure : failures) {
                for (GoogleStorageLocation logLocation : failure.failedLogLocations()) {
                    Blob log = storage.get(logLocation.asBlobId());
                    if (log != null) {
                        LOGGER.error("========================================== start {} ==========================================", log.getName());
                        LOGGER.error(new String(log.getContent()));
                        LOGGER.error("========================================== end {} ==========================================", log.getName());
                    }
                    else {
                        LOGGER.error("run log [{}] missing", logLocation);
                    }
                }
            }
        }
    }
}
