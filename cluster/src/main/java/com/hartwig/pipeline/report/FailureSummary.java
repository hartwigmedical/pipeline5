package com.hartwig.pipeline.report;

import java.util.List;
import java.util.stream.Collectors;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.PipelineState;
import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.execution.PipelineStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FailureSummary {

    private static final Logger LOGGER = LoggerFactory.getLogger(FailureSummary.class);

    public static void of(final Storage storage, final PipelineState state) {
        List<StageOutput> failures = state.stageOutputs()
                .stream()
                .filter(stageOutput -> stageOutput.status().equals(PipelineStatus.FAILED))
                .collect(Collectors.toList());
        if (!failures.isEmpty()) {
            LOGGER.error("Failures in pipeline stages. Printing each failures full run.log here");
            for (StageOutput failure : failures) {
                for (Blob log : failure.logs()) {
                    LOGGER.error("========================================== start {} ==========================================",
                            log.getName());
                    LOGGER.error(new String(log.getContent()));
                    LOGGER.error("========================================== end {} ==========================================",
                            log.getName());
                    ;
                }
            }
        }
    }
}
