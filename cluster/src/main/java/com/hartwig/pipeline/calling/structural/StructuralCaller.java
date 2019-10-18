package com.hartwig.pipeline.calling.structural;

import static java.lang.String.format;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.cloud.storage.Storage;
import com.google.common.collect.ImmutableList;
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
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.InputDownload;
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
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;

public class StructuralCaller implements Stage<StructuralCallerOutput, SomaticRunMetadata> {
    public static final String NAMESPACE = "structural_caller";

    private final InputDownload referenceBam;
    private final InputDownload referenceBai;
    private final InputDownload tumorBam;
    private final InputDownload tumorBai;

    private String filteredVcf;
    private String fullVcfCompressed;
    private String annotatedOutputFilePath;

    public StructuralCaller(final AlignmentPair pair) {
        referenceBam = new InputDownload(pair.reference().finalBamLocation());
        referenceBai = new InputDownload(pair.reference().finalBaiLocation());
        tumorBam = new InputDownload(pair.tumor().finalBamLocation());
        tumorBai = new InputDownload(pair.tumor().finalBaiLocation());
    }

    @Override
    public List<BashCommand> inputs() {
        return ImmutableList.of(referenceBam, referenceBai, tumorBam, tumorBai);
    }

    @Override
    public List<ResourceDownload> resources(final Storage storage, final String resourceBucket, final RuntimeBucket bucket) {
        return ImmutableList.of(ResourceDownload.from(bucket, new Resource(storage, resourceBucket, ResourceNames.REFERENCE_GENOME)),
                ResourceDownload.from(bucket, new Resource(storage, resourceBucket, ResourceNames.GRIDSS_CONFIG)),
                ResourceDownload.from(bucket, new Resource(storage, resourceBucket, ResourceNames.GRIDSS_PON)));
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> commands(final SomaticRunMetadata metadata, final Map<String, ResourceDownload> resources) {
        List<BashCommand> commands = new ArrayList<>();
        commands.add(new ExportVariableCommand("PATH", format("${PATH}:%s", dirname(new BwaCommand().asBash()))));

        String tumorSampleName = metadata.tumor().sampleName();
        String referenceSampleName = metadata.reference().sampleName();
        String jointName = referenceSampleName + "_" + tumorSampleName;

        String referenceWorkingDir = format("%s/%s.gridss.working", VmDirectories.OUTPUT, basename(referenceBam.getLocalTargetPath()));
        String tumorWorkingDir = format("%s/%s.gridss.working", VmDirectories.OUTPUT, basename(tumorBam.getLocalTargetPath()));

        String configurationFile = resources.get(ResourceNames.GRIDSS_CONFIG).find("properties");
        String blacklist = resources.get(ResourceNames.GRIDSS_CONFIG).find("bed");

        String refBamPath = referenceBam.getLocalTargetPath();
        String tumorBamPath = tumorBam.getLocalTargetPath();
        String referenceGenomePath = resources.get(ResourceNames.REFERENCE_GENOME).find("fasta");
        commands.addAll(new Preprocess(refBamPath,
                referenceWorkingDir,
                referenceSampleName,
                referenceGenomePath).apply(SubStageInputOutput.empty(referenceSampleName)).bash());
        commands.addAll(new Preprocess(tumorBamPath, tumorWorkingDir, tumorSampleName, referenceGenomePath).apply(SubStageInputOutput.empty(
                tumorSampleName)).bash());

        Assemble assemble = new Assemble(refBamPath, tumorBamPath, jointName, referenceGenomePath, configurationFile, blacklist);
        String filteredVcfBasename = VmDirectories.outputFile(format("%s.gridss.somatic.vcf", tumorSampleName));
        String fullVcfBasename = VmDirectories.outputFile(format("%s.gridss.somatic.full.vcf", tumorSampleName));

        SubStageInputOutput filteredAndAnnotated =
                assemble.andThen(new Calling(refBamPath, tumorBamPath, referenceGenomePath, configurationFile, blacklist))
                        .andThen(new Annotation(referenceBam.getLocalTargetPath(),
                                tumorBam.getLocalTargetPath(),
                                assemble.completedBam(),
                                referenceGenomePath,
                                jointName,
                                configurationFile,
                                blacklist))
                        .andThen(new Filter(filteredVcfBasename, fullVcfBasename))
                        .apply(SubStageInputOutput.empty(jointName));
        commands.addAll(filteredAndAnnotated.bash());

        filteredVcf = filteredVcfBasename + "gz";
        fullVcfCompressed = fullVcfBasename + ".gz";
        annotatedOutputFilePath = VmDirectories.outputFile(jointName + ".annotation.vcf.gz");
        return commands;
    }

    private static String basename(String filename) {
        return new File(filename).getName();
    }

    private static String dirname(String filename) {
        return new File(filename).getParent();
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.structuralCalling(bash, resultsDirectory);
    }

    @Override
    public StructuralCallerOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        return StructuralCallerOutput.builder()
                .status(jobStatus)
                .maybeFilteredVcf(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(basename(filteredVcf))))
                .maybeFilteredVcfIndex(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(basename(filteredVcf + ".tbi"))))
                .maybeFullVcf(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(basename(fullVcfCompressed))))
                .maybeFullVcfIndex(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(basename(fullVcfCompressed + ".tbi"))))
                .addReportComponents(new ZippedVcfAndIndexComponent(bucket,
                        NAMESPACE,
                        Folder.from(),
                        basename(annotatedOutputFilePath),
                        format("%s.gridss.unfiltered.vcf.gz", metadata.tumor().sampleName()),
                        resultsDirectory))
                .addReportComponents(new ZippedVcfAndIndexComponent(bucket,
                        NAMESPACE,
                        Folder.from(),
                        basename(fullVcfCompressed),
                        basename(fullVcfCompressed),
                        resultsDirectory))
                .addReportComponents(new ZippedVcfAndIndexComponent(bucket,
                        NAMESPACE,
                        Folder.from(),
                        basename(filteredVcf),
                        format("%s.gridss.somatic.vcf.gz", metadata.tumor().sampleName()),
                        resultsDirectory))
                .addReportComponents(new EntireOutputComponent(bucket,
                        Folder.from(),
                        NAMESPACE,
                        resultsDirectory,
                        s -> !s.contains("working") || s.endsWith("sorted.bam.sv.bam") || s.endsWith("sorted.bam.sv.bai")))
                .addReportComponents(new RunLogComponent(bucket, NAMESPACE, Folder.from(), resultsDirectory))
                .addReportComponents(new StartupScriptComponent(bucket, NAMESPACE, Folder.from()))

                .build();
    }

    @Override
    public StructuralCallerOutput skippedOutput(final SomaticRunMetadata metadata) {
        return StructuralCallerOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runStructuralCaller();
    }
}
