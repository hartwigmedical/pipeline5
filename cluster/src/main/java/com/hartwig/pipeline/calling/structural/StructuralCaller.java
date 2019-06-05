package com.hartwig.pipeline.calling.structural;

import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.alignment.after.metrics.BamMetricsOutput;
import com.hartwig.pipeline.calling.structural.gridss.GridssCommon;
import com.hartwig.pipeline.calling.structural.gridss.command.GridssToBashCommandConverter;
import com.hartwig.pipeline.calling.structural.gridss.command.IdentifyVariants;
import com.hartwig.pipeline.calling.structural.gridss.stage.*;
import com.hartwig.pipeline.execution.JobStatus;
import com.hartwig.pipeline.execution.vm.*;
import com.hartwig.pipeline.io.GoogleStorageLocation;
import com.hartwig.pipeline.io.ResultsDirectory;
import com.hartwig.pipeline.io.RuntimeBucket;
import com.hartwig.pipeline.report.EntireOutputComponent;
import com.hartwig.pipeline.resource.Resource;
import com.hartwig.pipeline.resource.ResourceNames;
import com.hartwig.pipeline.trace.StageTrace;

import java.io.File;

import static java.lang.String.format;
import static java.util.Arrays.asList;

public class StructuralCaller {

    private static final String NAMESPACE = "structural_caller";

    private final Arguments arguments;
    private final ComputeEngine computeEngine;
    private final Storage storage;
    private final ResultsDirectory resultsDirectory;

    StructuralCaller(final Arguments arguments, final ComputeEngine computeEngine, final Storage storage,
            final ResultsDirectory resultsDirectory) {
        this.arguments = arguments;
        this.computeEngine = computeEngine;
        this.storage = storage;
        this.resultsDirectory = resultsDirectory;
    }

    public StructuralCallerOutput run(AlignmentPair pair, BamMetricsOutput metricsOutput, BamMetricsOutput mateMetricsOutput) {
        if (!arguments.runStructuralCaller()) {
            return StructuralCallerOutput.builder().status(JobStatus.SKIPPED).build();
        }

        StageTrace trace = new StageTrace(NAMESPACE, StageTrace.ExecutorType.COMPUTE_ENGINE).start();

        String tumorSampleName = pair.tumor().sample().name();
        String referenceSampleName = pair.reference().sample().name();
        RuntimeBucket runtimeBucket = RuntimeBucket.from(storage, NAMESPACE, referenceSampleName, tumorSampleName, arguments);
        BashStartupScript bash = BashStartupScript.of(runtimeBucket.name());

        ResourceDownload referenceGenomeDownload =
                ResourceDownload.from(runtimeBucket, new Resource(storage, arguments.resourceBucket(), ResourceNames.REFERENCE_GENOME));
        String referenceGenomePath = referenceGenomeDownload.find("fa", "fasta");

        InputDownload tumorBam = new InputDownload(pair.tumor().finalBamLocation());
        InputDownload tumorBai = new InputDownload(pair.tumor().finalBaiLocation());
        InputDownload referenceBam = new InputDownload(pair.reference().finalBamLocation());
        InputDownload referenceBai = new InputDownload(pair.reference().finalBaiLocation());
        InputDownload metricsDownload = new InputDownload(metricsOutput.metricsOutputFile());

        InputDownload mateMetricsDownload = new InputDownload(mateMetricsOutput.metricsOutputFile());

        bash.addCommands(asList(tumorBam, referenceBam, tumorBai, referenceBai, metricsDownload, mateMetricsDownload, referenceGenomeDownload));

        bash.addLine("ulimit -n 102400");
        bash.addLine(format("gsutil -qm cp gs://common-resources/gridss_config/gridss.properties %s", GridssCommon.configFile()));
        bash.addLine(format("gsutil -qm cp gs://common-resources/gridss_config/ENCFF001TDO.bed %s", GridssCommon.blacklist()));

        String gridssWorkingDirForReferenceBam =
                format("%s/%s.gridss.working", VmDirectories.OUTPUT, basename(referenceBam.getLocalTargetPath()));
        String gridssWorkingDirForTumorBam = format("%s/%s.gridss.working", VmDirectories.OUTPUT, basename(tumorBam.getLocalTargetPath()));

        bash.addCommand(new MkDirCommand(GridssCommon.tmpDir()));
        bash.addCommand(new MkDirCommand(gridssWorkingDirForReferenceBam));
        bash.addCommand(new MkDirCommand(gridssWorkingDirForTumorBam));

        String preprocessSvOutputReferenceBam =
                format("%s/%s.sv.bam", gridssWorkingDirForReferenceBam, basename(referenceBam.getLocalTargetPath()));
        String preprocessSvOutputTumorBam = format("%s/%s.sv.bam", gridssWorkingDirForTumorBam, basename(tumorBam.getLocalTargetPath()));

        CommandFactory commandFactory = new CommandFactory();
        GridssToBashCommandConverter commandConverter = new GridssToBashCommandConverter();

        Preprocess.PreprocessResult preprocessedSample = new Preprocess(commandFactory, commandConverter).initialise(referenceBam.getLocalTargetPath(),
                referenceSampleName,
                referenceGenomePath,
                metricsDownload.getLocalTargetPath(),
                preprocessSvOutputReferenceBam);
        Preprocess.PreprocessResult preprocessedTumor = new Preprocess(commandFactory, commandConverter).initialise(tumorBam.getLocalTargetPath(),
                tumorSampleName,
                referenceGenomePath,
                mateMetricsDownload.getLocalTargetPath(),
                preprocessSvOutputTumorBam);

        Assemble.AssembleResult assemblyResult =
                new Assemble(commandFactory, commandConverter).initialise(preprocessedSample.svBam(), preprocessedTumor.svBam(), referenceGenomePath);

        IdentifyVariants calling = commandFactory.buildIdentifyVariants(referenceBam.getLocalTargetPath(),
                tumorBam.getLocalTargetPath(),
                assemblyResult.assemblyBam(),
                referenceGenomePath,
                GridssCommon.blacklist());

        Annotation.AnnotationResult annotationResult = new Annotation(commandFactory, commandConverter).initialise(referenceBam.getLocalTargetPath(),
                tumorBam.getLocalTargetPath(),
                assemblyResult.assemblyBam(),
                calling.resultantVcf(),
                referenceGenomePath);

        Filter.FilterResult filterResult = new Filter().initialise(annotationResult.annotatedVcf(), basename(tumorBam.getLocalTargetPath()));

        bash.addCommands(preprocessedSample.commands())
                .addCommands(preprocessedTumor.commands())
                .addCommands(assemblyResult.commands())
                .addCommand(commandConverter.convert(calling))
                .addCommands(annotationResult.commands())
                .addCommands(filterResult.commands());

        bash.addCommand(new OutputUpload(GoogleStorageLocation.of(runtimeBucket.name(), resultsDirectory.path())));
        JobStatus status = computeEngine.submit(runtimeBucket, VirtualMachineJobDefinition.structuralCalling(bash, resultsDirectory));
        trace.stop();
        return StructuralCallerOutput.builder()
                .status(status)
                .maybeFilteredVcf(GoogleStorageLocation.of(runtimeBucket.name(), resultsDirectory.path("annotated.vcf.gz")))
                .maybeFilteredVcfIndex(GoogleStorageLocation.of(runtimeBucket.name(), resultsDirectory.path("annotated.vcf.gz.tbi")))
                .maybeFullVcf(GoogleStorageLocation.of(runtimeBucket.name(), resultsDirectory.path("annotated.vcf.gz")))
                .maybeFullVcfIndex(GoogleStorageLocation.of(runtimeBucket.name(), resultsDirectory.path("annotated.vcf.gz.tbi")))
                .addReportComponents(new EntireOutputComponent(runtimeBucket, pair, NAMESPACE, resultsDirectory))
                .build();
    }

    private static String basename(String filename) {
        return new File(filename).getName();
    }
}
