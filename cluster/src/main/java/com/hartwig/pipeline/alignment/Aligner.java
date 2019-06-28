package com.hartwig.pipeline.alignment;

import static java.lang.String.format;

import static com.hartwig.pipeline.alignment.AlignmentOutputPaths.bai;
import static com.hartwig.pipeline.alignment.AlignmentOutputPaths.bam;
import static com.hartwig.pipeline.alignment.AlignmentOutputPaths.sorted;
import static com.hartwig.pipeline.resource.ResourceNames.REFERENCE_GENOME;

import com.google.cloud.storage.Storage;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.dataproc.ClusterOptimizer;
import com.hartwig.pipeline.execution.dataproc.JarLocation;
import com.hartwig.pipeline.execution.dataproc.JarUpload;
import com.hartwig.pipeline.execution.dataproc.SparkExecutor;
import com.hartwig.pipeline.execution.dataproc.SparkJobDefinition;
import com.hartwig.pipeline.io.BamComposer;
import com.hartwig.pipeline.io.ResultsDirectory;
import com.hartwig.pipeline.io.RuntimeBucket;
import com.hartwig.pipeline.io.SampleUpload;
import com.hartwig.pipeline.io.sources.SampleData;
import com.hartwig.pipeline.io.sources.SampleSource;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.report.SingleFileComponent;
import com.hartwig.pipeline.resource.ReferenceGenomeAlias;
import com.hartwig.pipeline.resource.Resource;
import com.hartwig.pipeline.trace.StageTrace;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Aligner {

    public final static String NAMESPACE = "aligner";

    private static final Logger LOGGER = LoggerFactory.getLogger(Aligner.class);

    private final Arguments arguments;
    private final Storage storage;
    private final SampleSource sampleSource;
    private final SampleUpload sampleUpload;
    private final SparkExecutor dataproc;
    private final JarUpload jarUpload;
    private final ClusterOptimizer clusterOptimizer;
    private final ResultsDirectory resultsDirectory;
    private final AlignmentOutputStorage alignmentOutputStorage;

    Aligner(final Arguments arguments, final Storage storage, final SampleSource sampleSource, final SampleUpload sampleUpload,
            final SparkExecutor dataproc, final JarUpload jarUpload, final ClusterOptimizer clusterOptimizer,
            final ResultsDirectory resultsDirectory, final AlignmentOutputStorage alignmentOutputStorage) {
        this.arguments = arguments;
        this.storage = storage;
        this.sampleSource = sampleSource;
        this.sampleUpload = sampleUpload;
        this.dataproc = dataproc;
        this.jarUpload = jarUpload;
        this.clusterOptimizer = clusterOptimizer;
        this.resultsDirectory = resultsDirectory;
        this.alignmentOutputStorage = alignmentOutputStorage;
    }

    public AlignmentOutput run(SingleSampleRunMetadata metadata) throws Exception {

        if (!arguments.runAligner()) {
            return alignmentOutputStorage.get(metadata)
                    .orElseThrow(() -> new IllegalArgumentException(format(
                            "Unable to find output for sample [%s]. Please run the aligner first by setting -run_aligner to true",
                            arguments.sampleId())));
        }

        StageTrace trace = new StageTrace(NAMESPACE, StageTrace.ExecutorType.DATAPROC).start();

        SampleData sampleData = sampleSource.sample(metadata, arguments);
        Sample sample = sampleData.sample();

        RuntimeBucket runtimeBucket = RuntimeBucket.from(storage, NAMESPACE, metadata, arguments);
        new Resource(storage, arguments.resourceBucket(), REFERENCE_GENOME, new ReferenceGenomeAlias()).copyInto(runtimeBucket);
        if (arguments.upload()) {
            sampleUpload.run(sample, runtimeBucket);
        }
        JarLocation jarLocation = jarUpload.run(runtimeBucket, arguments);

        runJob(Jobs.noStatusCheck(dataproc), SparkJobDefinition.gunzip(jarLocation, runtimeBucket), runtimeBucket);
        runJob(Jobs.statusCheckGoogleStorage(dataproc, resultsDirectory),
                SparkJobDefinition.bamCreation(jarLocation, arguments, runtimeBucket, clusterOptimizer.optimize(sampleData)),
                runtimeBucket);

        compose(sample, runtimeBucket);

        runJob(Jobs.noStatusCheck(dataproc),
                SparkJobDefinition.sortAndIndex(jarLocation, arguments, runtimeBucket, sample, resultsDirectory),
                runtimeBucket);

        AlignmentOutput alignmentOutput = alignmentOutputStorage.get(metadata)
                .orElseThrow(() -> new RuntimeException("No results found in Google Storage for sample"));
        trace.stop();
        return AlignmentOutput.builder()
                .from(alignmentOutput)
                .addReportComponents(new DataprocLogComponent(sample, runtimeBucket, resultsDirectory),
                        new SingleFileComponent(runtimeBucket,
                                NAMESPACE,
                                Folder.from(metadata),
                                sorted(sample.name()),
                                bam(sample.name()),
                                resultsDirectory),
                        new SingleFileComponent(runtimeBucket,
                                NAMESPACE,
                                Folder.from(metadata),
                                bai(sorted(sample.name())),
                                bai(bam(sample.name())),
                                resultsDirectory))
                .build();
    }

    private void compose(final Sample sample, final RuntimeBucket runtimeBucket) {
        new BamComposer(resultsDirectory, 32, "").run(sample, runtimeBucket);
    }

    private void runJob(SparkExecutor executor, SparkJobDefinition jobDefinition, RuntimeBucket runtimeBucket) {
        PipelineStatus result = executor.submit(runtimeBucket, jobDefinition);
        if (result.equals(PipelineStatus.FAILED)) {
            throw new RuntimeException(format("Job [%s] reported status failed. Check prior error messages or job logs on Google dataproc",
                    jobDefinition.name()));
        } else {
            LOGGER.debug("Job [{}] completed successfully", jobDefinition.name());
        }
    }
}