package com.hartwig.pipeline;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import com.hartwig.pipeline.alignment.Aligner;
import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.calling.germline.GermlineCaller;
import com.hartwig.pipeline.calling.germline.GermlineCallerOutput;
import com.hartwig.pipeline.flagstat.Flagstat;
import com.hartwig.pipeline.flagstat.FlagstatOutput;
import com.hartwig.pipeline.metadata.SingleSampleEventListener;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.metrics.BamMetrics;
import com.hartwig.pipeline.metrics.BamMetricsOutput;
import com.hartwig.pipeline.report.PipelineResults;
import com.hartwig.pipeline.snpgenotype.SnpGenotype;
import com.hartwig.pipeline.snpgenotype.SnpGenotypeOutput;
import com.hartwig.pipeline.stages.StageRunner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SingleSamplePipeline {

    private static final Logger LOGGER = LoggerFactory.getLogger(SingleSamplePipeline.class);

    private final SingleSampleEventListener eventListener;
    private final StageRunner<SingleSampleRunMetadata> stageRunner;
    private final Aligner aligner;
    private final PipelineResults report;
    private final ExecutorService executorService;
    private final Arguments arguments;

    SingleSamplePipeline(final SingleSampleEventListener sampleMetadataApi, final StageRunner<SingleSampleRunMetadata> stageRunner,
            final Aligner aligner, final PipelineResults report, final ExecutorService executorService, final Arguments arguments) {
        this.eventListener = sampleMetadataApi;
        this.stageRunner = stageRunner;
        this.aligner = aligner;
        this.report = report;
        this.executorService = executorService;
        this.arguments = arguments;
    }

    public PipelineState run(SingleSampleRunMetadata metadata) throws Exception {
        LOGGER.info("Pipeline5 single sample pipeline starting for sample name [{}] with id [{}] {}",
                metadata.sampleName(),
                metadata.sampleId(),
                arguments.runId().map(runId -> String.format("using run tag [%s]", runId)).orElse(""));
        PipelineState state = new PipelineState();
        AlignmentOutput alignmentOutput = report.add(state.add(aligner.run(metadata)));
        eventListener.alignmentComplete(state);
        if (state.shouldProceed()) {

            Future<BamMetricsOutput> bamMetricsFuture =
                    executorService.submit(() -> stageRunner.run(metadata, new BamMetrics(alignmentOutput)));
            Future<SnpGenotypeOutput> unifiedGenotyperFuture =
                    executorService.submit(() -> stageRunner.run(metadata, new SnpGenotype(alignmentOutput)));
            Future<FlagstatOutput> flagstatOutputFuture =
                    executorService.submit(() -> stageRunner.run(metadata, new Flagstat(alignmentOutput)));

            if (metadata.type().equals(SingleSampleRunMetadata.SampleType.REFERENCE)) {
                Future<GermlineCallerOutput> germlineCallerFuture =
                        executorService.submit(() -> stageRunner.run(metadata, new GermlineCaller(alignmentOutput)));
                report.add(state.add(futurePayload(germlineCallerFuture)));
            }

            report.add(state.add(futurePayload(bamMetricsFuture)));
            report.add(state.add(futurePayload(unifiedGenotyperFuture)));
            report.add(state.add(futurePayload(flagstatOutputFuture)));
            report.compose(metadata, state);
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
