package com.hartwig.pipeline;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import com.hartwig.pipeline.alignment.Aligner;
import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.alignment.after.metrics.BamMetrics;
import com.hartwig.pipeline.alignment.after.metrics.BamMetricsOutput;
import com.hartwig.pipeline.calling.germline.GermlineCaller;
import com.hartwig.pipeline.calling.germline.GermlineCallerOutput;
import com.hartwig.pipeline.flagstat.Flagstat;
import com.hartwig.pipeline.flagstat.FlagstatOutput;
import com.hartwig.pipeline.metadata.PatientMetadata;
import com.hartwig.pipeline.metadata.PatientMetadataApi;
import com.hartwig.pipeline.report.PatientReport;
import com.hartwig.pipeline.snpgenotype.SnpGenotype;
import com.hartwig.pipeline.snpgenotype.SnpGenotypeOutput;
import com.hartwig.pipeline.tools.Versions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SingleSamplePipeline {

    private static final Logger LOGGER = LoggerFactory.getLogger(SingleSamplePipeline.class);

    private final PatientMetadataApi patientMetadataApi;
    private final Aligner aligner;
    private final BamMetrics metrics;
    private final GermlineCaller germlineCaller;
    private final SnpGenotype snpGenotype;
    private final Flagstat flagstat;
    private final PatientReport report;
    private final ExecutorService executorService;
    private final Arguments arguments;

    SingleSamplePipeline(final PatientMetadataApi patientMetadataApi, final Aligner aligner, final BamMetrics metrics,
            final GermlineCaller germlineCaller, final SnpGenotype snpGenotype, final Flagstat flagstat, final PatientReport report,
            final ExecutorService executorService, final Arguments arguments) {
        this.patientMetadataApi = patientMetadataApi;
        this.aligner = aligner;
        this.metrics = metrics;
        this.germlineCaller = germlineCaller;
        this.snpGenotype = snpGenotype;
        this.flagstat = flagstat;
        this.report = report;
        this.executorService = executorService;
        this.arguments = arguments;
    }

    public PipelineState run() throws Exception {
        Versions.printAll();
        PatientMetadata metadata = patientMetadataApi.getMetadata();
        String setName = metadata.setName();
        LOGGER.info("Pipeline5 single sample pipeline starting for sample [{}] in set [{}] {}",
                metadata.sample(),
                setName,
                arguments.runId().map(runId -> String.format("with run id [%s]", runId)).orElse(""));
        PipelineState state = new PipelineState();
        AlignmentOutput alignmentOutput = report.add(state.add(aligner.run()));
        if (state.shouldProceed()) {

            Future<BamMetricsOutput> bamMetricsFuture = executorService.submit(() -> metrics.run(alignmentOutput));
            Future<GermlineCallerOutput> germlineCallerFuture = executorService.submit(() -> germlineCaller.run(alignmentOutput));
            Future<SnpGenotypeOutput> unifiedGenotyperFuture = executorService.submit(() -> snpGenotype.run(alignmentOutput));
            Future<FlagstatOutput> flagstatOutputFuture = executorService.submit(() -> flagstat.run(alignmentOutput));

            report.add(state.add(futurePayload(bamMetricsFuture)));
            report.add(state.add(futurePayload(germlineCallerFuture)));
            report.add(state.add(futurePayload(unifiedGenotyperFuture)));
            report.add(state.add(futurePayload(flagstatOutputFuture)));
            report.compose(setName);
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
