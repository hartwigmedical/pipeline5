package com.hartwig.pipeline.calling.structural;

import static java.lang.String.format;
import static java.util.Arrays.asList;

import java.io.File;

import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.alignment.after.metrics.BamMetricsOutput;
import com.hartwig.pipeline.calling.structural.gridss.process.GridssCommon;
import com.hartwig.pipeline.calling.structural.gridss.process.IdentifyVariants;
import com.hartwig.pipeline.calling.structural.gridss.stage.Annotation;
import com.hartwig.pipeline.calling.structural.gridss.stage.Assemble;
import com.hartwig.pipeline.calling.structural.gridss.stage.CommandFactory;
import com.hartwig.pipeline.calling.structural.gridss.stage.Preprocess;
import com.hartwig.pipeline.execution.JobStatus;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.OutputUpload;
import com.hartwig.pipeline.execution.vm.ResourceDownload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.io.GoogleStorageLocation;
import com.hartwig.pipeline.io.ResultsDirectory;
import com.hartwig.pipeline.io.RuntimeBucket;
import com.hartwig.pipeline.resource.Resource;
import com.hartwig.pipeline.resource.ResourceNames;

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

        bash.addCommands(asList(tumorBam, referenceBam, tumorBai, referenceBai, metricsDownload, mateMetricsDownload));
        CommandFactory commandFactory = new CommandFactory();

        bash.addLine(format("gsutil -qm cp gs://common-resources/gridss_config/gridss.properties %s", GridssCommon.configFile()));
        bash.addLine(format("gsutil -qm cp gs://common-resources/gridss_config/ENCFF001TDO.bed %s", GridssCommon.blacklist()));
        bash.addLine(format("mkdir -p %s", GridssCommon.tmpDir()));

        String gridssWorkingDirForReferenceBam =
                format("%s/%s.gridss.working", VmDirectories.OUTPUT, basename(referenceBam.getLocalTargetPath()));
        String gridssWorkingDirForTumorBam = format("%s/%s.gridss.working", VmDirectories.OUTPUT, basename(tumorBam.getLocalTargetPath()));

        bash.addLine(format("mkdir -p %s", gridssWorkingDirForReferenceBam));
        bash.addLine(format("mkdir -p %s", gridssWorkingDirForTumorBam));

        String preprocessSvOutputReferenceBam =
                format("%s/%s.sv.bam", gridssWorkingDirForReferenceBam, basename(referenceBam.getLocalTargetPath()));
        String preprocessSvOutputTumorBam = format("%s/%s.sv.bam", gridssWorkingDirForTumorBam, basename(tumorBam.getLocalTargetPath()));

        Preprocess.PreprocessResult preprocessedSample = new Preprocess(commandFactory).initialise(referenceBam.getLocalTargetPath(),
                referenceSampleName,
                referenceGenomePath,
                metricsDownload.getLocalTargetPath(),
                preprocessSvOutputReferenceBam);
        Preprocess.PreprocessResult preprocessedTumor = new Preprocess(commandFactory).initialise(tumorBam.getLocalTargetPath(),
                tumorSampleName,
                referenceGenomePath,
                mateMetricsDownload.getLocalTargetPath(),
                preprocessSvOutputTumorBam);

        Assemble.AssembleResult assemblyResult =
                new Assemble(commandFactory).initialise(preprocessedSample.svBam(), preprocessedTumor.svBam(), referenceGenomePath);

        IdentifyVariants calling = commandFactory.buildIdentifyVariants(referenceBam.getLocalTargetPath(),
                tumorBam.getLocalTargetPath(),
                assemblyResult.assemblyBam(),
                referenceGenomePath,
                GridssCommon.blacklist());

        Annotation.AnnotationResult annotationResult = new Annotation(commandFactory).initialise(referenceBam.getLocalTargetPath(),
                tumorBam.getLocalTargetPath(),
                assemblyResult.assemblyBam(),
                calling.resultantVcf(),
                referenceGenomePath);

        bash.addCommand(referenceGenomeDownload)
                .addCommands(preprocessedSample.commands())
                .addCommands(preprocessedTumor.commands())
                .addCommands(assemblyResult.commands())
                .addCommand(calling)
                .addCommands(annotationResult.commands());

        bash.addCommand(new OutputUpload(GoogleStorageLocation.of(runtimeBucket.name(), resultsDirectory.path())));
        JobStatus status = computeEngine.submit(runtimeBucket, VirtualMachineJobDefinition.structuralCalling(bash, resultsDirectory));

        return StructuralCallerOutput.builder()
                .status(status)
                .maybeStructuralVcf(GoogleStorageLocation.of(runtimeBucket.name(), resultsDirectory.path()))
                .maybeSvRecoveryVcf(GoogleStorageLocation.of(runtimeBucket.name(), resultsDirectory.path()))
                .build();
    }

    private static String basename(String filename) {
        return new File(filename).getName();
    }
}
