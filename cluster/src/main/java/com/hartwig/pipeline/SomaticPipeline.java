package com.hartwig.pipeline;

import static com.hartwig.pipeline.resource.ResourceFilesFactory.buildResourceFiles;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.calling.germline.GermlineCallerOutput;
import com.hartwig.pipeline.calling.somatic.SageCaller;
import com.hartwig.pipeline.calling.somatic.SomaticCallerOutput;
import com.hartwig.pipeline.calling.structural.StructuralCaller;
import com.hartwig.pipeline.calling.structural.StructuralCallerOutput;
import com.hartwig.pipeline.metadata.SomaticMetadataApi;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.metrics.BamMetricsOutput;
import com.hartwig.pipeline.report.PipelineResults;
import com.hartwig.pipeline.rerun.PersistedAmber;
import com.hartwig.pipeline.rerun.PersistedCobalt;
import com.hartwig.pipeline.rerun.PersistedPurple;
import com.hartwig.pipeline.rerun.PersistedSage;
import com.hartwig.pipeline.rerun.PersistedStructuralCaller;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.StageRunner;
import com.hartwig.pipeline.tertiary.amber.Amber;
import com.hartwig.pipeline.tertiary.amber.AmberOutput;
import com.hartwig.pipeline.tertiary.bachelor.Bachelor;
import com.hartwig.pipeline.tertiary.bachelor.BachelorOutput;
import com.hartwig.pipeline.tertiary.chord.Chord;
import com.hartwig.pipeline.tertiary.chord.ChordOutput;
import com.hartwig.pipeline.tertiary.cobalt.Cobalt;
import com.hartwig.pipeline.tertiary.cobalt.CobaltOutput;
import com.hartwig.pipeline.tertiary.healthcheck.HealthCheckOutput;
import com.hartwig.pipeline.tertiary.healthcheck.HealthChecker;
import com.hartwig.pipeline.tertiary.linx.Linx;
import com.hartwig.pipeline.tertiary.linx.LinxOutput;
import com.hartwig.pipeline.tertiary.purple.Purple;
import com.hartwig.pipeline.tertiary.purple.PurpleOutput;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SomaticPipeline {

    private static final Logger LOGGER = LoggerFactory.getLogger(SomaticPipeline.class);

    private final Arguments arguments;
    private final StageRunner<SomaticRunMetadata> stageRunner;
    private final BlockingQueue<BamMetricsOutput> referenceBamMetricsOutputQueue;
    private final BlockingQueue<BamMetricsOutput> tumorBamMetricsOutputQueue;
    private final BlockingQueue<GermlineCallerOutput> germlineCallerOutputStorage;
    private final SomaticMetadataApi setMetadataApi;
    private final PipelineResults pipelineResults;
    private final ExecutorService executorService;

    SomaticPipeline(final Arguments arguments, final StageRunner<SomaticRunMetadata> stageRunner,
            final BlockingQueue<BamMetricsOutput> referenceBamMetricsOutputQueue,
            final BlockingQueue<BamMetricsOutput> tumorBamMetricsOutputQueue,
            final BlockingQueue<GermlineCallerOutput> germlineCallerOutputStorageQueue, final SomaticMetadataApi setMetadataApi,
            final PipelineResults pipelineResults, final ExecutorService executorService) {
        this.arguments = arguments;
        this.stageRunner = stageRunner;
        this.referenceBamMetricsOutputQueue = referenceBamMetricsOutputQueue;
        this.tumorBamMetricsOutputQueue = tumorBamMetricsOutputQueue;
        this.germlineCallerOutputStorage = germlineCallerOutputStorageQueue;
        this.setMetadataApi = setMetadataApi;
        this.pipelineResults = pipelineResults;
        this.executorService = executorService;
    }

    public PipelineState run(AlignmentPair pair) {

        PipelineState state = new PipelineState();

        SomaticRunMetadata metadata = setMetadataApi.get();
        LOGGER.info("Pipeline5 somatic pipeline starting for set [{}]", metadata.runName());

        final ResourceFiles resourceFiles = buildResourceFiles(arguments.refGenomeVersion());

        if (metadata.maybeTumor().isPresent()) {
            try {
                Future<AmberOutput> amberOutputFuture = executorService.submit(() -> stageRunner.run(metadata,
                        new PersistedAmber(new Amber(pair, resourceFiles), arguments, metadata.runName())));
                Future<CobaltOutput> cobaltOutputFuture = executorService.submit(() -> stageRunner.run(metadata,
                        new PersistedCobalt(new Cobalt(pair, resourceFiles), arguments, metadata.runName())));
                Future<SomaticCallerOutput> sageCallerOutputFuture = executorService.submit(() -> stageRunner.run(metadata,
                        new PersistedSage(new SageCaller(pair, resourceFiles), arguments, metadata.runName())));
                Future<StructuralCallerOutput> structuralCallerOutputFuture = executorService.submit(() -> stageRunner.run(metadata,
                        new PersistedStructuralCaller(new StructuralCaller(pair, resourceFiles), arguments, metadata.runName())));
                AmberOutput amberOutput = pipelineResults.add(state.add(amberOutputFuture.get()));
                CobaltOutput cobaltOutput = pipelineResults.add(state.add(cobaltOutputFuture.get()));
                SomaticCallerOutput sageOutput = pipelineResults.add(state.add(sageCallerOutputFuture.get()));
                StructuralCallerOutput structuralCallerOutput = pipelineResults.add(state.add(structuralCallerOutputFuture.get()));

                if (state.shouldProceed()) {
                    Future<PurpleOutput> purpleOutputFuture = executorService.submit(() -> pipelineResults.add(state.add(stageRunner.run(
                            metadata,
                            new PersistedPurple(new Purple(resourceFiles,
                                    sageOutput,
                                    structuralCallerOutput,
                                    amberOutput,
                                    cobaltOutput,
                                    arguments.shallow()), arguments, metadata.runName())))));
                    PurpleOutput purpleOutput = purpleOutputFuture.get();
                    if (state.shouldProceed()) {
                        BamMetricsOutput tumorMetrics = pollOrThrow(tumorBamMetricsOutputQueue, "tumor metrics");
                        BamMetricsOutput referenceMetrics = pollOrThrow(referenceBamMetricsOutputQueue, "reference metrics");

                        Future<HealthCheckOutput> healthCheckOutputFuture = executorService.submit(() -> stageRunner.run(metadata,
                                new HealthChecker(referenceMetrics, tumorMetrics, amberOutput, purpleOutput)));
                        Future<LinxOutput> linxOutputFuture =
                                executorService.submit(() -> stageRunner.run(metadata, new Linx(purpleOutput, resourceFiles)));
                        Future<BachelorOutput> bachelorOutputFuture = executorService.submit(() -> stageRunner.run(metadata,
                                new Bachelor(resourceFiles,
                                        purpleOutput,
                                        pair.tumor(),
                                        pollOrThrow(germlineCallerOutputStorage, "germline"))));
                        Future<ChordOutput> chordOutputFuture =
                                executorService.submit(() -> stageRunner.run(metadata, new Chord(purpleOutput)));
                        pipelineResults.add(state.add(healthCheckOutputFuture.get()));
                        pipelineResults.add(state.add(linxOutputFuture.get()));
                        pipelineResults.add(state.add(bachelorOutputFuture.get()));
                        pipelineResults.add(state.add(chordOutputFuture.get()));
                        pipelineResults.compose(metadata);
                    }
                }
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
        return state;
    }

    public static <T> T pollOrThrow(final BlockingQueue<T> tumourBamMetricsOutput, final String name) throws InterruptedException {
        T poll = tumourBamMetricsOutput.poll(24, TimeUnit.HOURS);
        if (poll == null) {
            throw new RuntimeException(String.format("No results from single sample pipeline within 24 hours for [%s]", name));
        }
        return poll;
    }
}
