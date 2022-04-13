package com.hartwig.pipeline;

import static com.hartwig.pipeline.resource.ResourceFilesFactory.buildResourceFiles;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import com.hartwig.pipeline.alignment.Aligner;
import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.calling.germline.GermlineCaller;
import com.hartwig.pipeline.calling.germline.GermlineCallerOutput;
import com.hartwig.pipeline.cram.CramConversion;
import com.hartwig.pipeline.cram.CramOutput;
import com.hartwig.pipeline.cram2bam.Cram2Bam;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.flagstat.Flagstat;
import com.hartwig.pipeline.flagstat.FlagstatOutput;
import com.hartwig.pipeline.metadata.SingleSampleEventListener;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.metrics.BamMetrics;
import com.hartwig.pipeline.metrics.BamMetricsOutput;
import com.hartwig.pipeline.report.PipelineResults;
import com.hartwig.pipeline.reruns.PersistedDataset;
import com.hartwig.pipeline.resource.ResourceFiles;
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
    private final PersistedDataset persistedDataset;
    private final BlockingQueue<BamMetricsOutput> metricsOutputQueue;
    private final BlockingQueue<FlagstatOutput> flagstatOutputQueue;
    private final BlockingQueue<GermlineCallerOutput> germlineCallerOutputQueue;

    SingleSamplePipeline(final SingleSampleEventListener eventListener, final StageRunner<SingleSampleRunMetadata> stageRunner,
            final Aligner aligner, final PipelineResults report, final ExecutorService executorService, final Arguments arguments,
            final PersistedDataset persistedDataset, final BlockingQueue<BamMetricsOutput> metricsOutputQueue,
            final BlockingQueue<FlagstatOutput> flagstatOutputQueue, final BlockingQueue<GermlineCallerOutput> germlineCallerOutputQueue) {
        this.eventListener = eventListener;
        this.stageRunner = stageRunner;
        this.aligner = aligner;
        this.report = report;
        this.executorService = executorService;
        this.arguments = arguments;
        this.persistedDataset = persistedDataset;
        this.metricsOutputQueue = metricsOutputQueue;
        this.flagstatOutputQueue = flagstatOutputQueue;
        this.germlineCallerOutputQueue = germlineCallerOutputQueue;
    }

    public PipelineState run(SingleSampleRunMetadata metadata) throws Exception {
        LOGGER.info("Pipeline5 single sample pipeline starting for sample name [{}] with id [{}] {}",
                metadata.sampleName(),
                metadata.barcode(),
                arguments.runId().map(runId -> String.format("using run tag [%s]", runId)).orElse(""));
        PipelineState state = new PipelineState();
        final ResourceFiles resourceFiles = buildResourceFiles(arguments);
        AlignmentOutput alignmentOutput = convertCramsIfNecessary(arguments, metadata, state);
        eventListener.alignmentComplete(alignmentOutput);
        if (state.shouldProceed()) {
            Future<BamMetricsOutput> bamMetricsFuture = executorService.submit(() -> stageRunner.run(metadata,
                    new BamMetrics(resourceFiles, alignmentOutput, persistedDataset, arguments)));
            Future<SnpGenotypeOutput> unifiedGenotyperFuture =
                    executorService.submit(() -> stageRunner.run(metadata, new SnpGenotype(resourceFiles, alignmentOutput)));
            Future<FlagstatOutput> flagstatOutputFuture =
                    executorService.submit(() -> stageRunner.run(metadata, new Flagstat(alignmentOutput, persistedDataset)));
            Future<CramOutput> cramOutputFuture = executorService.submit(() -> stageRunner.run(metadata,
                    new CramConversion(alignmentOutput, metadata.type(), resourceFiles)));

            if (metadata.type().equals(SingleSampleRunMetadata.SampleType.REFERENCE)) {
                Future<GermlineCallerOutput> germlineCallerFuture = executorService.submit(() -> stageRunner.run(metadata,
                        new GermlineCaller(alignmentOutput, resourceFiles, persistedDataset)));
                GermlineCallerOutput germlineCallerOutput = futurePayload(germlineCallerFuture);
                germlineCallerOutputQueue.put(germlineCallerOutput);
                report.add(state.add(germlineCallerOutput));
            }

            BamMetricsOutput bamMetricsOutput = futurePayload(bamMetricsFuture);
            metricsOutputQueue.put(bamMetricsOutput);
            FlagstatOutput flagstatOutput = futurePayload(flagstatOutputFuture);
            flagstatOutputQueue.put(flagstatOutput);
            report.add(state.add(bamMetricsOutput));
            report.add(state.add(futurePayload(unifiedGenotyperFuture)));
            report.add(state.add(flagstatOutput));
            report.add(state.add(futurePayload(cramOutputFuture)));
            report.compose(metadata, "SingleSample");
            eventListener.complete(state);
        }
        return state;
    }

    private AlignmentOutput convertCramsIfNecessary(final Arguments arguments, final SingleSampleRunMetadata metadata,
            final PipelineState state) throws Exception {
        AlignmentOutput alignmentOutput = report.add(state.add(aligner.run(metadata)));
        alignmentOutput =
                state.shouldProceed() && !arguments.useCrams() && alignmentOutput.alignments().path().endsWith(FileTypes.CRAM) ? state.add(
                        stageRunner.run(metadata, new Cram2Bam(alignmentOutput.alignments(), metadata.type()))) : alignmentOutput;
        return alignmentOutput;
    }

    private static <T> T futurePayload(final Future<T> future) {
        try {
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
