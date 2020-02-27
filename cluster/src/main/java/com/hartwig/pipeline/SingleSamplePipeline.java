package com.hartwig.pipeline;

import static com.hartwig.pipeline.resource.ResourceFilesFactory.buildResourceFiles;

import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.alignment.vm.VmAligner;
import com.hartwig.pipeline.calling.germline.GermlineCaller;
import com.hartwig.pipeline.calling.germline.GermlineCallerOutput;
import com.hartwig.pipeline.cram.CramConversion;
import com.hartwig.pipeline.cram.CramOutput;
import com.hartwig.pipeline.flagstat.Flagstat;
import com.hartwig.pipeline.flagstat.FlagstatOutput;
import com.hartwig.pipeline.metadata.SingleSampleEventListener;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.metrics.BamMetrics;
import com.hartwig.pipeline.metrics.BamMetricsOutput;
import com.hartwig.pipeline.report.PipelineResults;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.snpgenotype.SnpGenotype;
import com.hartwig.pipeline.snpgenotype.SnpGenotypeOutput;
import com.hartwig.pipeline.stages.StageRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class SingleSamplePipeline {

    private static final Logger LOGGER = LoggerFactory.getLogger(SingleSamplePipeline.class);

    private final SingleSampleEventListener eventListener;
    private final StageRunner<SingleSampleRunMetadata> stageRunner;
    private final VmAligner aligner;
    private final PipelineResults report;
    private final ExecutorService executorService;
    private final Boolean isStandalone;
    private final Arguments arguments;

    SingleSamplePipeline(final SingleSampleEventListener sampleMetadataApi, final StageRunner<SingleSampleRunMetadata> stageRunner,
                         final VmAligner aligner, final PipelineResults report, final ExecutorService executorService, final Boolean isStandalone,
                         final Arguments arguments) {
        this.eventListener = sampleMetadataApi;
        this.stageRunner = stageRunner;
        this.aligner = aligner;
        this.report = report;
        this.executorService = executorService;
        this.isStandalone = isStandalone;
        this.arguments = arguments;
    }

    public PipelineState run(SingleSampleRunMetadata metadata) throws Exception {
        LOGGER.info("Pipeline5 single sample pipeline starting for sample name [{}] with id [{}] {}",
                metadata.sampleName(),
                metadata.sampleId(),
                arguments.runId().map(runId -> String.format("using run tag [%s]", runId)).orElse(""));
        PipelineState state = new PipelineState();
        final ResourceFiles resourceFiles = buildResourceFiles(arguments.refGenomeVersion());
        AlignmentOutput alignmentOutput = report.add(state.add(aligner.run(metadata)));
        eventListener.alignmentComplete(state);
        if (state.shouldProceed()) {

            Future<BamMetricsOutput> bamMetricsFuture =
                    executorService.submit(() -> stageRunner.run(metadata, new BamMetrics(alignmentOutput)));
            Future<SnpGenotypeOutput> unifiedGenotyperFuture =
                    executorService.submit(() -> stageRunner.run(metadata, new SnpGenotype(resourceFiles, alignmentOutput)));
            Future<FlagstatOutput> flagstatOutputFuture =
                    executorService.submit(() -> stageRunner.run(metadata, new Flagstat(alignmentOutput)));
            Future<CramOutput> cramOutputFuture =
                    executorService.submit(() -> stageRunner.run(metadata, new CramConversion(alignmentOutput)));

            if (metadata.type().equals(SingleSampleRunMetadata.SampleType.REFERENCE)) {
                Future<GermlineCallerOutput> germlineCallerFuture =
                        executorService.submit(() -> stageRunner.run(metadata, new GermlineCaller(alignmentOutput, resourceFiles)));
                report.add(state.add(futurePayload(germlineCallerFuture)));
            }

            report.add(state.add(futurePayload(bamMetricsFuture)));
            report.add(state.add(futurePayload(unifiedGenotyperFuture)));
            report.add(state.add(futurePayload(flagstatOutputFuture)));
            report.add(state.add(futurePayload(cramOutputFuture)));
            report.compose(metadata, isStandalone, state);
            eventListener.complete(state);
        }
        return state;
    }

    private static <T> T futurePayload(final Future<T> future) {
        try {
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
