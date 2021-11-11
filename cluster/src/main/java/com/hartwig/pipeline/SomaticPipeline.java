package com.hartwig.pipeline;

import static com.hartwig.pipeline.resource.ResourceFilesFactory.buildResourceFiles;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.calling.germline.GermlineCallerOutput;
import com.hartwig.pipeline.calling.sage.SageGermlineCaller;
import com.hartwig.pipeline.calling.sage.SageOutput;
import com.hartwig.pipeline.calling.sage.SageSomaticCaller;
import com.hartwig.pipeline.calling.structural.StructuralCaller;
import com.hartwig.pipeline.calling.structural.StructuralCallerOutput;
import com.hartwig.pipeline.calling.structural.StructuralCallerPostProcess;
import com.hartwig.pipeline.calling.structural.StructuralCallerPostProcessOutput;
import com.hartwig.pipeline.flagstat.FlagstatOutput;
import com.hartwig.pipeline.metadata.SomaticMetadataApi;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.metrics.BamMetricsOutput;
import com.hartwig.pipeline.report.PipelineResults;
import com.hartwig.pipeline.reruns.PersistedDataset;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.StageRunner;
import com.hartwig.pipeline.tertiary.amber.Amber;
import com.hartwig.pipeline.tertiary.amber.AmberOutput;
import com.hartwig.pipeline.tertiary.chord.Chord;
import com.hartwig.pipeline.tertiary.chord.ChordOutput;
import com.hartwig.pipeline.tertiary.cobalt.Cobalt;
import com.hartwig.pipeline.tertiary.cobalt.CobaltOutput;
import com.hartwig.pipeline.tertiary.cuppa.Cuppa;
import com.hartwig.pipeline.tertiary.cuppa.CuppaOutput;
import com.hartwig.pipeline.tertiary.healthcheck.HealthCheckOutput;
import com.hartwig.pipeline.tertiary.healthcheck.HealthChecker;
import com.hartwig.pipeline.tertiary.linx.Linx;
import com.hartwig.pipeline.tertiary.linx.LinxOutput;
import com.hartwig.pipeline.tertiary.orange.Orange;
import com.hartwig.pipeline.tertiary.orange.OrangeOutput;
import com.hartwig.pipeline.tertiary.peach.Peach;
import com.hartwig.pipeline.tertiary.peach.PeachOutput;
import com.hartwig.pipeline.tertiary.protect.Protect;
import com.hartwig.pipeline.tertiary.protect.ProtectOutput;
import com.hartwig.pipeline.tertiary.purple.Purple;
import com.hartwig.pipeline.tertiary.purple.PurpleOutput;
import com.hartwig.pipeline.tertiary.sigs.Sigs;
import com.hartwig.pipeline.tertiary.sigs.SigsOutput;
import com.hartwig.pipeline.tertiary.virus.VirusAnalysis;
import com.hartwig.pipeline.tertiary.virus.VirusOutput;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SomaticPipeline {

    private static final Logger LOGGER = LoggerFactory.getLogger(SomaticPipeline.class);

    private final Arguments arguments;
    private final StageRunner<SomaticRunMetadata> stageRunner;
    private final BlockingQueue<BamMetricsOutput> referenceBamMetricsOutputQueue;
    private final BlockingQueue<BamMetricsOutput> tumorBamMetricsOutputQueue;
    private final BlockingQueue<FlagstatOutput> referenceFlagstatOutputQueue;
    private final BlockingQueue<FlagstatOutput> tumorFlagstatOutputQueue;
    private final BlockingQueue<GermlineCallerOutput> germlineCallerOutputStorage;
    private final SomaticMetadataApi setMetadataApi;
    private final PipelineResults pipelineResults;
    private final ExecutorService executorService;
    private final PersistedDataset persistedDataset;

    SomaticPipeline(final Arguments arguments, final StageRunner<SomaticRunMetadata> stageRunner,
            final BlockingQueue<BamMetricsOutput> referenceBamMetricsOutputQueue,
            final BlockingQueue<BamMetricsOutput> tumorBamMetricsOutputQueue,
            final BlockingQueue<FlagstatOutput> referenceFlagstatOutputQueue, final BlockingQueue<FlagstatOutput> tumorFlagstatOutputQueue,
            final BlockingQueue<GermlineCallerOutput> germlineCallerOutputStorageQueue, final SomaticMetadataApi setMetadataApi,
            final PipelineResults pipelineResults, final ExecutorService executorService, final PersistedDataset persistedDataset) {
        this.arguments = arguments;
        this.stageRunner = stageRunner;
        this.referenceBamMetricsOutputQueue = referenceBamMetricsOutputQueue;
        this.tumorBamMetricsOutputQueue = tumorBamMetricsOutputQueue;
        this.referenceFlagstatOutputQueue = referenceFlagstatOutputQueue;
        this.tumorFlagstatOutputQueue = tumorFlagstatOutputQueue;
        this.germlineCallerOutputStorage = germlineCallerOutputStorageQueue;
        this.setMetadataApi = setMetadataApi;
        this.pipelineResults = pipelineResults;
        this.executorService = executorService;
        this.persistedDataset = persistedDataset;
    }

    public PipelineState run(AlignmentPair pair) {
        PipelineState state = new PipelineState();

        SomaticRunMetadata metadata = setMetadataApi.get();
        LOGGER.info("Pipeline5 somatic pipeline starting for set [{}]", metadata.set());

        final ResourceFiles resourceFiles = buildResourceFiles(arguments);

        if (metadata.maybeTumor().isPresent()) {
            try {
                Future<AmberOutput> amberOutputFuture =
                        executorService.submit(() -> stageRunner.run(metadata, new Amber(pair, resourceFiles, persistedDataset)));
                Future<CobaltOutput> cobaltOutputFuture =
                        executorService.submit(() -> stageRunner.run(metadata, new Cobalt(pair, resourceFiles, persistedDataset)));
                Future<SageOutput> sageSomaticOutputFuture = executorService.submit(() -> stageRunner.run(metadata,
                        new SageSomaticCaller(pair, resourceFiles, persistedDataset, arguments.shallow())));
                Future<SageOutput> sageGermlineOutputFuture = executorService.submit(() -> stageRunner.run(metadata,
                        new SageGermlineCaller(pair, resourceFiles, persistedDataset)));
                Future<StructuralCallerOutput> structuralCallerOutputFuture = executorService.submit(() -> stageRunner.run(metadata,
                        new StructuralCaller(pair, resourceFiles, persistedDataset)));

                AmberOutput amberOutput = pipelineResults.add(state.add(amberOutputFuture.get()));
                CobaltOutput cobaltOutput = pipelineResults.add(state.add(cobaltOutputFuture.get()));
                SageOutput sageSomaticOutput = pipelineResults.add(state.add(sageSomaticOutputFuture.get()));
                SageOutput sageGermlineOutput = pipelineResults.add(state.add(sageGermlineOutputFuture.get()));

                StructuralCallerOutput structuralCallerOutput = pipelineResults.add(state.add(structuralCallerOutputFuture.get()));
                if (state.shouldProceed()) {
                    Future<StructuralCallerPostProcessOutput> structuralCallerPostProcessOutputFuture =
                            executorService.submit(() -> stageRunner.run(metadata,
                                    new StructuralCallerPostProcess(resourceFiles, structuralCallerOutput, persistedDataset)));
                    StructuralCallerPostProcessOutput structuralCallerPostProcessOutput =
                            pipelineResults.add(state.add(structuralCallerPostProcessOutputFuture.get()));

                    if (state.shouldProceed()) {
                        Future<PurpleOutput> purpleOutputFuture =
                                executorService.submit(() -> pipelineResults.add(state.add(stageRunner.run(metadata,
                                        new Purple(resourceFiles,
                                                sageSomaticOutput,
                                                sageGermlineOutput,
                                                structuralCallerPostProcessOutput,
                                                amberOutput,
                                                cobaltOutput,
                                                persistedDataset,
                                                arguments.shallow(),
                                                arguments.runSageGermlineCaller())))));
                        PurpleOutput purpleOutput = purpleOutputFuture.get();
                        if (state.shouldProceed()) {
                            BamMetricsOutput tumorMetrics = pollOrThrow(tumorBamMetricsOutputQueue, "tumor metrics");
                            BamMetricsOutput referenceMetrics = pollOrThrow(referenceBamMetricsOutputQueue, "reference metrics");
                            FlagstatOutput tumorFlagstat = pollOrThrow(tumorFlagstatOutputQueue, "tumor flagstat");
                            FlagstatOutput referenceFlagstat = pollOrThrow(referenceFlagstatOutputQueue, "reference flagstat");

                            Future<VirusOutput> virusOutputFuture = executorService.submit(() -> stageRunner.run(metadata,
                                    new VirusAnalysis(pair, resourceFiles, persistedDataset, purpleOutput, tumorMetrics)));

                            Future<HealthCheckOutput> healthCheckOutputFuture = executorService.submit(() -> stageRunner.run(metadata,
                                    new HealthChecker(referenceMetrics, tumorMetrics, referenceFlagstat, tumorFlagstat, purpleOutput)));
                            Future<LinxOutput> linxOutputFuture = executorService.submit(() -> stageRunner.run(metadata,
                                    new Linx(purpleOutput, resourceFiles, persistedDataset)));
                            Future<SigsOutput> signatureOutputFuture =
                                    executorService.submit(() -> stageRunner.run(metadata, new Sigs(purpleOutput, resourceFiles)));
                            Future<ChordOutput> chordOutputFuture = executorService.submit(() -> stageRunner.run(metadata,
                                    new Chord(arguments.refGenomeVersion(), purpleOutput, persistedDataset)));
                            pipelineResults.add(state.add(healthCheckOutputFuture.get()));
                            LinxOutput linxOutput = pipelineResults.add(state.add(linxOutputFuture.get()));
                            Future<CuppaOutput> cuppaOutputFuture = executorService.submit(() -> stageRunner.run(metadata,
                                    new Cuppa(purpleOutput, linxOutput, resourceFiles, persistedDataset)));
                            Future<PeachOutput> peachOutputFuture = executorService.submit(() -> stageRunner.run(metadata,
                                    new Peach(purpleOutput, resourceFiles, persistedDataset)));
                            VirusOutput virusOutput = pipelineResults.add(state.add(virusOutputFuture.get()));
                            ChordOutput chordOutput = pipelineResults.add(state.add(chordOutputFuture.get()));
                            CuppaOutput cuppaOutput = pipelineResults.add(state.add(cuppaOutputFuture.get()));
                            PeachOutput peachOutput = pipelineResults.add(state.add(peachOutputFuture.get()));
                            ProtectOutput protectOutput = pipelineResults.add(state.add(executorService.submit(() -> stageRunner.run(
                                    metadata,
                                    new Protect(purpleOutput, linxOutput, virusOutput, chordOutput, resourceFiles, persistedDataset)))
                                    .get()));

                            Future<OrangeOutput> orangeOutputFuture = executorService.submit(() -> stageRunner.run(metadata,
                                    new Orange(tumorMetrics,
                                            referenceMetrics,
                                            tumorFlagstat,
                                            referenceFlagstat,
                                            sageSomaticOutput,
                                            sageGermlineOutput,
                                            purpleOutput,
                                            chordOutput,
                                            linxOutput,
                                            cuppaOutput,
                                            virusOutput,
                                            protectOutput,
                                            peachOutput,
                                            resourceFiles)));
                            pipelineResults.add(state.add(signatureOutputFuture.get()));
                            pipelineResults.add(state.add(orangeOutputFuture.get()));
                            pipelineResults.compose(metadata);
                        }
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
