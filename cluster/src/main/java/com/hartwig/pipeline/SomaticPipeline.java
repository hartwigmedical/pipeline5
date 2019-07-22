package com.hartwig.pipeline;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.alignment.AlignmentOutputStorage;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.calling.somatic.SomaticCaller;
import com.hartwig.pipeline.calling.somatic.SomaticCallerOutput;
import com.hartwig.pipeline.calling.structural.StructuralCaller;
import com.hartwig.pipeline.calling.structural.StructuralCallerOutput;
import com.hartwig.pipeline.cleanup.Cleanup;
import com.hartwig.pipeline.metadata.SomaticMetadataApi;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.metrics.BamMetricsOutput;
import com.hartwig.pipeline.metrics.BamMetricsOutputStorage;
import com.hartwig.pipeline.report.FullSomaticResults;
import com.hartwig.pipeline.report.PipelineResults;
import com.hartwig.pipeline.tertiary.amber.Amber;
import com.hartwig.pipeline.tertiary.amber.AmberOutput;
import com.hartwig.pipeline.tertiary.cobalt.Cobalt;
import com.hartwig.pipeline.tertiary.cobalt.CobaltOutput;
import com.hartwig.pipeline.tertiary.healthcheck.HealthChecker;
import com.hartwig.pipeline.tertiary.purple.Purple;
import com.hartwig.pipeline.tertiary.purple.PurpleOutput;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SomaticPipeline {

    private static final Logger LOGGER = LoggerFactory.getLogger(SomaticPipeline.class);

    private final AlignmentOutputStorage alignmentOutputStorage;
    private final BamMetricsOutputStorage bamMetricsOutputStorage;
    private final SomaticMetadataApi setMetadataApi;
    private final PipelineResults pipelineResults;
    private final FullSomaticResults fullSomaticResults;
    private final Cleanup cleanup;
    private final Amber amber;
    private final Cobalt cobalt;
    private final SomaticCaller somaticCaller;
    private final StructuralCaller structuralCaller;
    private final Purple purple;
    private final HealthChecker healthChecker;
    private final ExecutorService executorService;

    SomaticPipeline(final AlignmentOutputStorage alignmentOutputStorage, final BamMetricsOutputStorage bamMetricsOutputStorage,
            final SomaticMetadataApi setMetadataApi, final PipelineResults pipelineResults, final FullSomaticResults fullSomaticResults,
            final Cleanup cleanup, final Amber amber, final Cobalt cobalt, final SomaticCaller somaticCaller,
            final StructuralCaller structuralCaller, final Purple purple, final HealthChecker healthChecker,
            final ExecutorService executorService) {
        this.alignmentOutputStorage = alignmentOutputStorage;
        this.bamMetricsOutputStorage = bamMetricsOutputStorage;
        this.setMetadataApi = setMetadataApi;
        this.pipelineResults = pipelineResults;
        this.fullSomaticResults = fullSomaticResults;
        this.cleanup = cleanup;
        this.amber = amber;
        this.cobalt = cobalt;
        this.somaticCaller = somaticCaller;
        this.structuralCaller = structuralCaller;
        this.purple = purple;
        this.healthChecker = healthChecker;
        this.executorService = executorService;
    }

    public PipelineState run() {

        PipelineState state = new PipelineState();

        SomaticRunMetadata metadata = setMetadataApi.get();
        LOGGER.info("Pipeline5 somatic pipeline starting for set [{}]", metadata.runName());

        AlignmentOutput referenceAlignmentOutput =
                alignmentOutputStorage.get(metadata.reference()).orElseThrow(throwIllegalState(metadata.reference().sampleId()));
        AlignmentOutput tumorAlignmentOutput =
                alignmentOutputStorage.get(metadata.tumor()).orElseThrow(throwIllegalState(metadata.tumor().sampleName()));
        AlignmentPair pair = AlignmentPair.of(referenceAlignmentOutput, tumorAlignmentOutput);

        try {
            Future<AmberOutput> amberOutputFuture = executorService.submit(() -> amber.run(metadata, pair));
            Future<CobaltOutput> cobaltOutputFuture = executorService.submit(() -> cobalt.run(metadata, pair));
            Future<SomaticCallerOutput> somaticCallerOutputFuture = executorService.submit(() -> somaticCaller.run(metadata, pair));
            Future<StructuralCallerOutput> structuralCallerOutputFuture =
                    executorService.submit(() -> structuralCaller.run(metadata, pair));
            AmberOutput amberOutput = pipelineResults.add(state.add(amberOutputFuture.get()));
            CobaltOutput cobaltOutput = pipelineResults.add(state.add(cobaltOutputFuture.get()));
            SomaticCallerOutput somaticCallerOutput = pipelineResults.add(state.add(somaticCallerOutputFuture.get()));
            StructuralCallerOutput structuralCallerOutput = pipelineResults.add(state.add(structuralCallerOutputFuture.get()));

            if (state.shouldProceed()) {
                Future<PurpleOutput> purpleOutputFuture = executorService.submit(() -> pipelineResults.add(state.add(purple.run(metadata,
                        pair,
                        somaticCallerOutput,
                        structuralCallerOutput,
                        cobaltOutput,
                        amberOutput))));
                PurpleOutput purpleOutput = purpleOutputFuture.get();
                if (state.shouldProceed()) {
                    BamMetricsOutput tumorMetrics = bamMetricsOutputStorage.get(metadata.tumor());
                    BamMetricsOutput referenceMetrics = bamMetricsOutputStorage.get(metadata.reference());
                    pipelineResults.add(state.add(healthChecker.run(metadata,
                            pair,
                            tumorMetrics,
                            referenceMetrics,
                            amberOutput,
                            purpleOutput)));
                    pipelineResults.compose(metadata);
                    fullSomaticResults.compose(metadata);
                }
            }
            setMetadataApi.complete(state.status(), metadata);
            if (state.shouldProceed()) {
                cleanup.run(metadata);
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
        return state;
    }

    @NotNull
    private static Supplier<RuntimeException> throwIllegalState(String sample) {
        return () -> new IllegalStateException(String.format(
                "No alignment output found for sample [%s]. Has the single sample pipeline been run?",
                sample));
    }
}
