package com.hartwig.pipeline.calling.structural;

import static java.lang.String.format;

import static com.hartwig.pipeline.resource.ResourceNames.GRIDSS_CONFIG;
import static com.hartwig.pipeline.resource.ResourceNames.GRIDSS_REPEAT_MASKER_DB;
import static com.hartwig.pipeline.resource.ResourceNames.VIRUS_REFERENCE_GENOME;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.calling.SubStageInputOutput;
import com.hartwig.pipeline.calling.command.BwaCommand;
import com.hartwig.pipeline.calling.command.VersionedToolCommand;
import com.hartwig.pipeline.calling.structural.gridss.stage.Driver;
import com.hartwig.pipeline.calling.structural.gridss.stage.Filter;
import com.hartwig.pipeline.calling.structural.gridss.stage.RepeatMaskerInsertionAnnotation;
import com.hartwig.pipeline.calling.structural.gridss.stage.ViralAnnotation;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.InputDownload;
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
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tools.Versions;

public class StructuralCaller implements Stage<StructuralCallerOutput, SomaticRunMetadata> {
    public static final String NAMESPACE = "structural_caller";

    private final InputDownload referenceBam;
    private final InputDownload referenceBai;
    private final InputDownload tumorBam;
    private final InputDownload tumorBai;

    private String filteredVcf;
    private String fullVcfCompressed;

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
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> commands(final SomaticRunMetadata metadata) {
        List<BashCommand> commands = new ArrayList<>();
        commands.add(new ExportVariableCommand("PATH", format("${PATH}:%s", dirname(new BwaCommand().asBash()))));
        commands.add(new ExportVariableCommand("PATH",
                format("${PATH}:%s", dirname(new VersionedToolCommand("samtools", "samtools", Versions.SAMTOOLS).asBash()))));

        String tumorSampleName = metadata.tumor().sampleName();
        String refBamPath = referenceBam.getLocalTargetPath();
        String tumorBamPath = tumorBam.getLocalTargetPath();

        String configurationFilePath = Resource.of(GRIDSS_CONFIG, "gridss.properties");
        String blacklistBedPath = Resource.of(GRIDSS_CONFIG, "ENCFF001TDO.bed");
        String referenceGenomePath = Resource.REFERENCE_GENOME_FASTA;
        String virusReferenceGenomePath = Resource.of(VIRUS_REFERENCE_GENOME, "human_virus.fa");
        String repeatMaskerDbPath = Resource.of(GRIDSS_REPEAT_MASKER_DB, "hg19.fa.out");

        String filteredVcfBasename = VmDirectories.outputFile(format("%s.gridss.somatic.vcf", tumorSampleName));

        Driver driver = new Driver(VmDirectories.outputFile(tumorSampleName + ".assembly.bam"),
                referenceGenomePath,
                blacklistBedPath,
                configurationFilePath,
                refBamPath,
                tumorBamPath);
        SubStageInputOutput fullSomaticVcf = driver.apply(SubStageInputOutput.empty(tumorSampleName));

        SubStageInputOutput filteredAndAnnotated =
                new RepeatMaskerInsertionAnnotation(repeatMaskerDbPath).andThen(new ViralAnnotation(virusReferenceGenomePath))
                        .andThen(new Filter(filteredVcfBasename, fullSomaticVcf.outputFile().path()))
                        .apply(fullSomaticVcf);
        commands.addAll(filteredAndAnnotated.bash());

        filteredVcf = filteredVcfBasename + ".gz";
        fullVcfCompressed = fullSomaticVcf.outputFile().path() + ".gz";
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
