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
import com.hartwig.pipeline.metadata.SampleMetadataApi;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.results.PipelineResults;
import com.hartwig.pipeline.snpgenotype.SnpGenotype;
import com.hartwig.pipeline.snpgenotype.SnpGenotypeOutput;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SingleSamplePipeline {

    private static final Logger LOGGER = LoggerFactory.getLogger(SingleSamplePipeline.class);

    private final SampleMetadataApi sampleMetadataApi;
    private final Aligner aligner;
    private final BamMetrics metrics;
    private final GermlineCaller germlineCaller;
    private final SnpGenotype snpGenotype;
    private final Flagstat flagstat;
    private final PipelineResults report;
    private final ExecutorService executorService;
    private final Arguments arguments;

    SingleSamplePipeline(final SampleMetadataApi sampleMetadataApi, final Aligner aligner, final BamMetrics metrics,
            final GermlineCaller germlineCaller, final SnpGenotype snpGenotype, final Flagstat flagstat, final PipelineResults report,
            final ExecutorService executorService, final Arguments arguments) {
        this.sampleMetadataApi = sampleMetadataApi;
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
        SingleSampleRunMetadata metadata = sampleMetadataApi.get();
        LOGGER.info("Pipeline5 single sample pipeline starting for sample name [{}] with id [{}] {}",
                metadata.sampleName(),
                metadata.sampleId(),
                arguments.runId().map(runId -> String.format("using run tag [%s]", runId)).orElse(""));
        PipelineState state = new PipelineState();
        AlignmentOutput alignmentOutput = report.add(state.add(aligner.run(metadata)));
        sampleMetadataApi.complete(state.status());
        if (state.shouldProceed()) {

            Future<BamMetricsOutput> bamMetricsFuture = executorService.submit(() -> metrics.run(metadata, alignmentOutput));
            Future<SnpGenotypeOutput> unifiedGenotyperFuture = executorService.submit(() -> snpGenotype.run(metadata, alignmentOutput));
            Future<FlagstatOutput> flagstatOutputFuture = executorService.submit(() -> flagstat.run(metadata, alignmentOutput));

            if (metadata.type().equals(SingleSampleRunMetadata.SampleType.REFERENCE)) {
                Future<GermlineCallerOutput> germlineCallerFuture =
                        executorService.submit(() -> germlineCaller.run(metadata, alignmentOutput));
                report.add(state.add(futurePayload(germlineCallerFuture)));
            }

            report.add(state.add(futurePayload(bamMetricsFuture)));
            report.add(state.add(futurePayload(unifiedGenotyperFuture)));
            report.add(state.add(futurePayload(flagstatOutputFuture)));
            report.compose(RunTag.apply(arguments, metadata.sampleId()));
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
