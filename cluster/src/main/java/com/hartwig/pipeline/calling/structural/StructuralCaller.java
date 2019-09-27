package com.hartwig.pipeline.calling.structural;

import static java.lang.String.format;
import static java.util.Arrays.asList;

import static com.hartwig.pipeline.execution.PipelineStatus.SKIPPED;

import java.io.File;

import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.calling.SubStageInputOutput;
import com.hartwig.pipeline.calling.command.BwaCommand;
import com.hartwig.pipeline.calling.structural.gridss.stage.Annotation;
import com.hartwig.pipeline.calling.structural.gridss.stage.Assemble;
import com.hartwig.pipeline.calling.structural.gridss.stage.Calling;
import com.hartwig.pipeline.calling.structural.gridss.stage.Filter;
import com.hartwig.pipeline.calling.structural.gridss.stage.Preprocess;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.BatchInputDownload;
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.OutputUpload;
import com.hartwig.pipeline.execution.vm.ResourceDownload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.execution.vm.unix.ExportVariableCommand;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.report.EntireOutputComponent;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.report.RunLogComponent;
import com.hartwig.pipeline.report.StartupScriptComponent;
import com.hartwig.pipeline.report.ZippedVcfAndIndexComponent;
import com.hartwig.pipeline.resource.Resource;
import com.hartwig.pipeline.resource.ResourceNames;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.trace.StageTrace;

public class StructuralCaller {

    public static final String NAMESPACE = "structural_caller";

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

    public StructuralCallerOutput run(final SomaticRunMetadata metadata, final AlignmentPair pair) {
        if (!arguments.runStructuralCaller()) {
            return StructuralCallerOutput.builder().status(SKIPPED).build();
        }

        StageTrace trace = new StageTrace(NAMESPACE, metadata.runName(), StageTrace.ExecutorType.COMPUTE_ENGINE).start();

        String jointName = metadata.reference().sampleName() + "_" + metadata.tumor().sampleName();
        String tumorSampleName = pair.tumor().sample();
        String referenceSampleName = pair.reference().sample();
        RuntimeBucket runtimeBucket = RuntimeBucket.from(storage, NAMESPACE, metadata, arguments);
        BashStartupScript bash = BashStartupScript.of(runtimeBucket.name());

        ResourceDownload referenceGenomeDownload =
                ResourceDownload.from(runtimeBucket, new Resource(storage, arguments.resourceBucket(), ResourceNames.REFERENCE_GENOME));
        String referenceGenomePath = referenceGenomeDownload.find("fa", "fasta");
        ResourceDownload gridssConfigFiles =
                ResourceDownload.from(runtimeBucket, new Resource(storage, arguments.resourceBucket(), ResourceNames.GRIDSS_CONFIG));
        ResourceDownload gridssPonFiles =
                ResourceDownload.from(runtimeBucket, new Resource(storage, arguments.resourceBucket(), ResourceNames.GRIDSS_PON));

        InputDownload tumorBam = new InputDownload(pair.tumor().finalBamLocation());
        InputDownload tumorBai = new InputDownload(pair.tumor().finalBaiLocation());
        InputDownload referenceBam = new InputDownload(pair.reference().finalBamLocation());
        InputDownload referenceBai = new InputDownload(pair.reference().finalBaiLocation());

        bash.addCommand(new BatchInputDownload(referenceBam, referenceBai, tumorBam, tumorBai));
        bash.addCommands(asList(referenceGenomeDownload, gridssConfigFiles, gridssPonFiles));

        bash.addCommand(new ExportVariableCommand("PATH", format("${PATH}:%s", dirname(new BwaCommand().asBash()))));

        String referenceWorkingDir = format("%s/%s.gridss.working", VmDirectories.OUTPUT, basename(referenceBam.getLocalTargetPath()));
        String tumorWorkingDir = format("%s/%s.gridss.working", VmDirectories.OUTPUT, basename(tumorBam.getLocalTargetPath()));

        String configurationFile = gridssConfigFiles.find("properties");
        String blacklist = gridssConfigFiles.find("bed");

        String refBamPath = referenceBam.getLocalTargetPath();
        String tumorBamPath = tumorBam.getLocalTargetPath();
        SubStageInputOutput referencePreProcessed =
                new Preprocess(refBamPath, referenceWorkingDir, referenceSampleName, referenceGenomePath).apply(SubStageInputOutput.empty(
                        referenceSampleName));
        SubStageInputOutput tumorPreProcessed =
                new Preprocess(tumorBamPath, tumorWorkingDir, tumorSampleName, referenceGenomePath).apply(SubStageInputOutput.empty(
                        tumorSampleName));

        Assemble assemble = new Assemble(refBamPath, tumorBamPath, jointName, referenceGenomePath, configurationFile, blacklist);
        String filteredVcfBasename = VmDirectories.outputFile(format("%s.gridss.somatic.vcf", tumorSampleName));
        String fullVcfBasename = VmDirectories.outputFile(format("%s.gridss.somatic.full.vcf", tumorSampleName));

        SubStageInputOutput annotated =
                assemble.andThen(new Calling(refBamPath, tumorBamPath, referenceGenomePath, configurationFile, blacklist))
                        .andThen(new Annotation(referenceBam.getLocalTargetPath(),
                                tumorBam.getLocalTargetPath(),
                                assemble.completedBam(),
                                referenceGenomePath,
                                jointName,
                                configurationFile,
                                blacklist))
                        .apply(SubStageInputOutput.empty(jointName));

        SubStageInputOutput filtered = new Filter(filteredVcfBasename, fullVcfBasename).apply(annotated);
        bash.addCommands(referencePreProcessed.bash())
                .addCommands(tumorPreProcessed.bash())
                .addCommands(annotated.bash())
                .addCommands(filtered.bash())
                .addCommand(new OutputUpload(GoogleStorageLocation.of(runtimeBucket.name(), resultsDirectory.path())));

        PipelineStatus status = computeEngine.submit(runtimeBucket, VirtualMachineJobDefinition.structuralCalling(bash, resultsDirectory));

        trace.stop();

        String filteredVcf = filteredVcfBasename + ".gz";
        String fullVcfCompressed = fullVcfBasename + ".gz";
        return StructuralCallerOutput.builder()
                .status(status)
                .maybeFilteredVcf(GoogleStorageLocation.of(runtimeBucket.name(), resultsDirectory.path(basename(filteredVcf))))
                .maybeFilteredVcfIndex(GoogleStorageLocation.of(runtimeBucket.name(),
                        resultsDirectory.path(basename(filteredVcf + ".tbi"))))
                .maybeFullVcf(GoogleStorageLocation.of(runtimeBucket.name(), resultsDirectory.path(basename(fullVcfCompressed))))
                .maybeFullVcfIndex(GoogleStorageLocation.of(runtimeBucket.name(),
                        resultsDirectory.path(basename(fullVcfCompressed + ".tbi"))))
                .addReportComponents(new ZippedVcfAndIndexComponent(runtimeBucket,
                        NAMESPACE,
                        Folder.from(),
                        basename(annotated.outputFile().path()),
                        format("%s.gridss.unfiltered.vcf.gz", tumorSampleName),
                        resultsDirectory))
                .addReportComponents(new ZippedVcfAndIndexComponent(runtimeBucket,
                        NAMESPACE,
                        Folder.from(),
                        basename(fullVcfCompressed),
                        basename(fullVcfCompressed),
                        resultsDirectory))
                .addReportComponents(new ZippedVcfAndIndexComponent(runtimeBucket,
                        NAMESPACE,
                        Folder.from(),
                        basename(filteredVcf),
                        basename(filteredVcf),
                        resultsDirectory))
                .addReportComponents(new EntireOutputComponent(runtimeBucket,
                        Folder.from(),
                        NAMESPACE,
                        resultsDirectory,
                        s -> !s.contains("working") || s.endsWith("sorted.bam.sv.bam") || s.endsWith("sorted.bam.sv.bai")))
                .addReportComponents(new RunLogComponent(runtimeBucket, NAMESPACE, Folder.from(), resultsDirectory))
                .addReportComponents(new StartupScriptComponent(runtimeBucket, NAMESPACE, Folder.from()))
                .build();
    }

    private static String basename(String filename) {
        return new File(filename).getName();
    }

    private static String dirname(String filename) {
        return new File(filename).getParent();
    }
}
