package com.hartwig.pipeline;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import com.hartwig.patient.Sample;
import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.alignment.AlignmentOutputStorage;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.alignment.after.metrics.BamMetricsOutput;
import com.hartwig.pipeline.alignment.after.metrics.BamMetricsOutputStorage;
import com.hartwig.pipeline.calling.somatic.SomaticCaller;
import com.hartwig.pipeline.calling.somatic.SomaticCallerOutput;
import com.hartwig.pipeline.calling.structural.StructuralCaller;
import com.hartwig.pipeline.calling.structural.StructuralCallerOutput;
import com.hartwig.pipeline.cleanup.Cleanup;
import com.hartwig.pipeline.metadata.SetMetadata;
import com.hartwig.pipeline.metadata.SetMetadataApi;
import com.hartwig.pipeline.report.PatientReport;
import com.hartwig.pipeline.tertiary.amber.Amber;
import com.hartwig.pipeline.tertiary.amber.AmberOutput;
import com.hartwig.pipeline.tertiary.cobalt.Cobalt;
import com.hartwig.pipeline.tertiary.cobalt.CobaltOutput;
import com.hartwig.pipeline.tertiary.healthcheck.HealthChecker;
import com.hartwig.pipeline.tertiary.purple.Purple;
import com.hartwig.pipeline.tertiary.purple.PurpleOutput;

import org.jetbrains.annotations.NotNull;

public class SomaticPipeline {

    private final AlignmentOutputStorage alignmentOutputStorage;
    private final BamMetricsOutputStorage bamMetricsOutputStorage;
    private final SetMetadataApi setMetadataApi;
    private final PatientReport patientReport;
    private final Cleanup cleanup;
    private final Amber amber;
    private final Cobalt cobalt;
    private final SomaticCaller somaticCaller;
    private final StructuralCaller structuralCaller;
    private final Purple purple;
    private final HealthChecker healthChecker;
    private final ExecutorService executorService;

    SomaticPipeline(final AlignmentOutputStorage alignmentOutputStorage, final BamMetricsOutputStorage bamMetricsOutputStorage,
            final SetMetadataApi setMetadataApi, final PatientReport patientReport, final Cleanup cleanup, final Amber amber,
            final Cobalt cobalt, final SomaticCaller somaticCaller, final StructuralCaller structuralCaller, final Purple purple,
            final HealthChecker healthChecker, final ExecutorService executorService) {
        this.alignmentOutputStorage = alignmentOutputStorage;
        this.bamMetricsOutputStorage = bamMetricsOutputStorage;
        this.setMetadataApi = setMetadataApi;
        this.patientReport = patientReport;
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

        SetMetadata setMetadata = setMetadataApi.get();
        Sample tumorSample = setMetadata.tumor();
        AlignmentOutput referenceAlignmentOutput = alignmentOutputStorage.get(tumorSample).orElseThrow(throwIllegalState(tumorSample));
        Sample referenceSample = setMetadata.reference();
        AlignmentOutput tumorAlignmentOutput = alignmentOutputStorage.get(referenceSample).orElseThrow(throwIllegalState(referenceSample));
        AlignmentPair pair = AlignmentPair.of(tumorAlignmentOutput, referenceAlignmentOutput);

        try {
            Future<AmberOutput> amberOutputFuture = executorService.submit(() -> amber.run(pair));
            Future<CobaltOutput> cobaltOutputFuture = executorService.submit(() -> cobalt.run(pair));
            Future<SomaticCallerOutput> somaticCallerOutputFuture = executorService.submit(() -> somaticCaller.run(pair));
            Future<StructuralCallerOutput> structuralCallerOutputFuture = executorService.submit(() -> structuralCaller.run(pair));
            AmberOutput amberOutput = patientReport.add(state.add(amberOutputFuture.get()));
            CobaltOutput cobaltOutput = patientReport.add(state.add(cobaltOutputFuture.get()));
            SomaticCallerOutput somaticCallerOutput = patientReport.add(state.add(somaticCallerOutputFuture.get()));
            StructuralCallerOutput structuralCallerOutput = patientReport.add(state.add(structuralCallerOutputFuture.get()));

            if (state.shouldProceed()) {
                Future<PurpleOutput> purpleOutputFuture = executorService.submit(() -> patientReport.add(state.add(purple.run(pair,
                        somaticCallerOutput,
                        structuralCallerOutput,
                        cobaltOutput,
                        amberOutput))));
                PurpleOutput purpleOutput = purpleOutputFuture.get();
                if (state.shouldProceed()) {
                    BamMetricsOutput tumorMetrics = bamMetricsOutputStorage.get(tumorSample);
                    BamMetricsOutput referenceMetrics = bamMetricsOutputStorage.get(referenceSample);
                    patientReport.add(state.add(healthChecker.run(pair, tumorMetrics, referenceMetrics, amberOutput, purpleOutput)));
                    patientReport.compose(setMetadata.setName());
                    cleanup.run(pair);
                }
            }

        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
        return state;
    }

    @NotNull
    private static Supplier<RuntimeException> throwIllegalState(Sample sample) {
        return () -> new IllegalStateException(String.format(
                "No alignment output found for sample [%s]. Has the single sample pipeline been run?",
                sample.name()));
    }
}
