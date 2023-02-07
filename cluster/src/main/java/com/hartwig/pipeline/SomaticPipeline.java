package com.hartwig.pipeline;

import static com.hartwig.pipeline.resource.ResourceFilesFactory.buildResourceFiles;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.calling.sage.SageGermlineCaller;
import com.hartwig.pipeline.calling.sage.SageOutput;
import com.hartwig.pipeline.calling.sage.SageSomaticCaller;
import com.hartwig.pipeline.calling.structural.gridss.Gridss;
import com.hartwig.pipeline.calling.structural.gridss.GridssOutput;
import com.hartwig.pipeline.calling.structural.gripss.GripssGermline;
import com.hartwig.pipeline.calling.structural.gripss.GripssOutput;
import com.hartwig.pipeline.calling.structural.gripss.GripssSomatic;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.flagstat.FlagstatOutput;
import com.hartwig.pipeline.input.SomaticRunMetadata;
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
import com.hartwig.pipeline.tertiary.lilac.Lilac;
import com.hartwig.pipeline.tertiary.lilac.LilacBamSliceOutput;
import com.hartwig.pipeline.tertiary.lilac.LilacBamSlicer;
import com.hartwig.pipeline.tertiary.lilac.LilacOutput;
import com.hartwig.pipeline.tertiary.linx.LinxGermline;
import com.hartwig.pipeline.tertiary.linx.LinxGermlineOutput;
import com.hartwig.pipeline.tertiary.linx.LinxSomatic;
import com.hartwig.pipeline.tertiary.linx.LinxSomaticOutput;
import com.hartwig.pipeline.tertiary.orange.Orange;
import com.hartwig.pipeline.tertiary.orange.OrangeOutput;
import com.hartwig.pipeline.tertiary.pave.PaveGermline;
import com.hartwig.pipeline.tertiary.pave.PaveOutput;
import com.hartwig.pipeline.tertiary.pave.PaveSomatic;
import com.hartwig.pipeline.tertiary.peach.Peach;
import com.hartwig.pipeline.tertiary.peach.PeachOutput;
import com.hartwig.pipeline.tertiary.protect.Protect;
import com.hartwig.pipeline.tertiary.protect.ProtectOutput;
import com.hartwig.pipeline.tertiary.purple.Purple;
import com.hartwig.pipeline.tertiary.purple.PurpleOutput;
import com.hartwig.pipeline.tertiary.rose.Rose;
import com.hartwig.pipeline.tertiary.rose.RoseOutput;
import com.hartwig.pipeline.tertiary.sigs.Sigs;
import com.hartwig.pipeline.tertiary.sigs.SigsOutput;
import com.hartwig.pipeline.tertiary.virus.VirusBreakend;
import com.hartwig.pipeline.tertiary.virus.VirusBreakendOutput;
import com.hartwig.pipeline.tertiary.virus.VirusInterpreter;
import com.hartwig.pipeline.tertiary.virus.VirusInterpreterOutput;

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
    private final SomaticRunMetadata metadata;
    private final PipelineResults pipelineResults;
    private final ExecutorService executorService;
    private final PersistedDataset persistedDataset;

    SomaticPipeline(final Arguments arguments, final StageRunner<SomaticRunMetadata> stageRunner,
            final BlockingQueue<BamMetricsOutput> referenceBamMetricsOutputQueue,
            final BlockingQueue<BamMetricsOutput> tumorBamMetricsOutputQueue,
            final BlockingQueue<FlagstatOutput> referenceFlagstatOutputQueue, final BlockingQueue<FlagstatOutput> tumorFlagstatOutputQueue,
            final SomaticRunMetadata metadata, final PipelineResults pipelineResults, final ExecutorService executorService,
            final PersistedDataset persistedDataset) {
        this.arguments = arguments;
        this.stageRunner = stageRunner;
        this.referenceBamMetricsOutputQueue = referenceBamMetricsOutputQueue;
        this.tumorBamMetricsOutputQueue = tumorBamMetricsOutputQueue;
        this.referenceFlagstatOutputQueue = referenceFlagstatOutputQueue;
        this.tumorFlagstatOutputQueue = tumorFlagstatOutputQueue;
        this.metadata = metadata;
        this.pipelineResults = pipelineResults;
        this.executorService = executorService;
        this.persistedDataset = persistedDataset;
    }

    public PipelineState run(final AlignmentPair pair) {
        PipelineState state = new PipelineState();
        LOGGER.info("Pipeline5 somatic pipeline starting for set [{}]", metadata.set());

        final ResourceFiles resourceFiles = buildResourceFiles(arguments);
        try {
            Future<AmberOutput> amberOutputFuture =
                    executorService.submit(() -> stageRunner.run(metadata, new Amber(pair, resourceFiles, persistedDataset, arguments)));
            Future<CobaltOutput> cobaltOutputFuture =
                    executorService.submit(() -> stageRunner.run(metadata, new Cobalt(pair, resourceFiles, persistedDataset, arguments)));
            Future<SageOutput> sageSomaticOutputFuture = executorService.submit(() -> stageRunner.run(metadata,
                    new SageSomaticCaller(pair, persistedDataset, resourceFiles, arguments)));
            Future<SageOutput> sageGermlineOutputFuture =
                    executorService.submit(() -> stageRunner.run(metadata, new SageGermlineCaller(pair, persistedDataset, resourceFiles)));
            Future<GridssOutput> structuralCallerOutputFuture =
                    executorService.submit(() -> stageRunner.run(metadata, new Gridss(pair, resourceFiles, persistedDataset)));
            Future<VirusBreakendOutput> virusBreakendOutputFuture =
                    executorService.submit(() -> stageRunner.run(metadata, new VirusBreakend(pair, resourceFiles, persistedDataset)));

            SageOutput sageSomaticOutput = pipelineResults.add(state.add(sageSomaticOutputFuture.get()));
            SageOutput sageGermlineOutput = pipelineResults.add(state.add(sageGermlineOutputFuture.get()));

            AmberOutput amberOutput = pipelineResults.add(state.add(amberOutputFuture.get()));
            CobaltOutput cobaltOutput = pipelineResults.add(state.add(cobaltOutputFuture.get()));

            GridssOutput structuralCallerOutput = pipelineResults.add(state.add(structuralCallerOutputFuture.get()));

            if (state.shouldProceed()) {
                Future<PaveOutput> paveSomaticOutputFuture = executorService.submit(() -> stageRunner.run(metadata,
                        new PaveSomatic(resourceFiles, sageSomaticOutput, persistedDataset)));
                Future<PaveOutput> paveGermlineOutputFuture = executorService.submit(() -> stageRunner.run(metadata,
                        new PaveGermline(resourceFiles, sageGermlineOutput, persistedDataset)));
                PaveOutput paveSomaticOutput = pipelineResults.add(state.add(paveSomaticOutputFuture.get()));
                PaveOutput paveGermlineOutput = pipelineResults.add(state.add(paveGermlineOutputFuture.get()));

                Future<GripssOutput> gripssSomaticOutputFuture = executorService.submit(() -> stageRunner.run(metadata,
                        new GripssSomatic(structuralCallerOutput, persistedDataset, resourceFiles, arguments)));
                Future<GripssOutput> gripssGermlineOutputFuture = executorService.submit(() -> stageRunner.run(metadata,
                        new GripssGermline(structuralCallerOutput, persistedDataset, resourceFiles)));
                GripssOutput gripssSomaticProcessOutput = pipelineResults.add(state.add(gripssSomaticOutputFuture.get()));
                GripssOutput gripssGermlineProcessOutput = pipelineResults.add(state.add(gripssGermlineOutputFuture.get()));

                if (state.shouldProceed()) {
                    PurpleOutput purpleOutput = executorService.submit(() -> pipelineResults.add(state.add(stageRunner.run(metadata,
                            new Purple(resourceFiles,
                                    paveSomaticOutput,
                                    paveGermlineOutput,
                                    metadata.maybeTumor().map(t -> gripssSomaticProcessOutput).orElse(gripssGermlineProcessOutput),
                                    gripssGermlineProcessOutput,
                                    amberOutput,
                                    cobaltOutput,
                                    persistedDataset,
                                    arguments))))).get();

                    if (state.shouldProceed()) {
                        BamMetricsOutput tumorMetrics = metadata.maybeTumor()
                                .map(t -> pollOrThrow(tumorBamMetricsOutputQueue, "tumor metrics"))
                                .orElse(skippedMetrics(metadata.sampleName()));
                        BamMetricsOutput referenceMetrics = metadata.maybeReference()
                                .map(t -> pollOrThrow(referenceBamMetricsOutputQueue, "reference metrics"))
                                .orElse(skippedMetrics(metadata.sampleName()));
                        FlagstatOutput tumorFlagstat = metadata.maybeTumor()
                                .map(t -> pollOrThrow(tumorFlagstatOutputQueue, "tumor flagstat"))
                                .orElse(skippedFlagstat(metadata.sampleName()));
                        FlagstatOutput referenceFlagstat = metadata.maybeReference()
                                .map(t -> pollOrThrow(referenceFlagstatOutputQueue, "reference flagstat"))
                                .orElse(skippedFlagstat(metadata.sampleName()));

                        VirusBreakendOutput virusBreakendOutput = pipelineResults.add(state.add(virusBreakendOutputFuture.get()));
                        Future<VirusInterpreterOutput> virusInterpreterOutputFuture = executorService.submit(() -> stageRunner.run(metadata,
                                new VirusInterpreter(pair,
                                        resourceFiles,
                                        persistedDataset,
                                        virusBreakendOutput,
                                        purpleOutput,
                                        tumorMetrics)));

                        Future<HealthCheckOutput> healthCheckOutputFuture = executorService.submit(() -> stageRunner.run(metadata,
                                new HealthChecker(referenceMetrics, tumorMetrics, referenceFlagstat, tumorFlagstat, purpleOutput)));
                        Future<LilacBamSliceOutput> lilacBamSliceOutputFuture = executorService.submit(() -> stageRunner.run(metadata,
                                new LilacBamSlicer(pair, resourceFiles, persistedDataset)));
                        Future<LinxSomaticOutput> linxSomaticOutputFuture = executorService.submit(() -> stageRunner.run(metadata,
                                new LinxSomatic(purpleOutput, resourceFiles, persistedDataset)));
                        Future<LinxGermlineOutput> linxGermlineOutputFuture = executorService.submit(() -> stageRunner.run(metadata,
                                new LinxGermline(purpleOutput, resourceFiles, persistedDataset)));
                        LilacBamSliceOutput lilacBamSliceOutput = pipelineResults.add(state.add(lilacBamSliceOutputFuture.get()));
                        Future<LilacOutput> lilacOutputFuture = executorService.submit(() -> stageRunner.run(metadata,
                                new Lilac(lilacBamSliceOutput, resourceFiles, purpleOutput, persistedDataset)));
                        Future<SigsOutput> signatureOutputFuture =
                                executorService.submit(() -> stageRunner.run(metadata, new Sigs(purpleOutput, resourceFiles)));
                        Future<ChordOutput> chordOutputFuture = executorService.submit(() -> stageRunner.run(metadata,
                                new Chord(arguments.refGenomeVersion(), purpleOutput, persistedDataset)));
                        pipelineResults.add(state.add(healthCheckOutputFuture.get()));
                        LinxGermlineOutput linxGermlineOutput = pipelineResults.add(state.add(linxGermlineOutputFuture.get()));
                        LilacOutput lilacOutput = pipelineResults.add(state.add(lilacOutputFuture.get()));
                        LinxSomaticOutput linxSomaticOutput = pipelineResults.add(state.add(linxSomaticOutputFuture.get()));

                        Future<PeachOutput> peachOutputFuture = executorService.submit(() -> stageRunner.run(metadata,
                                new Peach(purpleOutput, resourceFiles, persistedDataset)));
                        ChordOutput chordOutput = pipelineResults.add(state.add(chordOutputFuture.get()));
                        PeachOutput peachOutput = pipelineResults.add(state.add(peachOutputFuture.get()));
                        VirusInterpreterOutput virusInterpreterOutput = pipelineResults.add(state.add(virusInterpreterOutputFuture.get()));
                        Future<CuppaOutput> cuppaOutputFuture = executorService.submit(() -> stageRunner.run(metadata,
                                new Cuppa(purpleOutput, linxSomaticOutput, virusInterpreterOutput, resourceFiles, persistedDataset)));
                        CuppaOutput cuppaOutput = pipelineResults.add(state.add(cuppaOutputFuture.get()));
                        ProtectOutput protectOutput = pipelineResults.add(state.add(executorService.submit(() -> stageRunner.run(metadata,
                                new Protect(purpleOutput,
                                        linxSomaticOutput,
                                        virusInterpreterOutput,
                                        chordOutput,
                                        lilacOutput,
                                        resourceFiles,
                                        persistedDataset))).get()));

                        Future<OrangeOutput> orangeOutputFuture = executorService.submit(() -> stageRunner.run(metadata,
                                new Orange(tumorMetrics,
                                        referenceMetrics,
                                        tumorFlagstat,
                                        referenceFlagstat,
                                        sageSomaticOutput,
                                        sageGermlineOutput,
                                        purpleOutput,
                                        chordOutput,
                                        lilacOutput,
                                        linxGermlineOutput,
                                        linxSomaticOutput,
                                        cuppaOutput,
                                        virusInterpreterOutput,
                                        protectOutput,
                                        peachOutput,
                                        resourceFiles)));
                        Future<RoseOutput> roseOutputFuture = executorService.submit(() -> stageRunner.run(metadata,
                                new Rose(resourceFiles,
                                        purpleOutput,
                                        linxSomaticOutput,
                                        virusInterpreterOutput,
                                        chordOutput,
                                        cuppaOutput)));
                        pipelineResults.add(state.add(signatureOutputFuture.get()));
                        pipelineResults.add(state.add(orangeOutputFuture.get()));
                        pipelineResults.add(state.add(roseOutputFuture.get()));

                        pipelineResults.compose(metadata, "Somatic");
                    }
                }
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
        return state;
    }

    private BamMetricsOutput skippedMetrics(final String sample) {
        return BamMetricsOutput.builder().sample(sample).status(PipelineStatus.SKIPPED).build();
    }

    private FlagstatOutput skippedFlagstat(final String sample) {
        return FlagstatOutput.builder().sample(sample).status(PipelineStatus.SKIPPED).build();
    }

    public static <T> T pollOrThrow(final BlockingQueue<T> tumourBamMetricsOutput, final String name) {
        try {
            T poll = tumourBamMetricsOutput.poll(24, TimeUnit.HOURS);
            if (poll == null) {
                throw new RuntimeException(String.format("No results from single sample pipeline within 24 hours for [%s]", name));
            }
            return poll;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
