package com.hartwig.pipeline.calling.structural;

import static java.lang.String.format;

import static com.hartwig.pipeline.resource.ResourceNames.GRIDSS_CONFIG;
import static com.hartwig.pipeline.resource.ResourceNames.VIRUS_REFERENCE_GENOME;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.calling.SubStageInputOutput;
import com.hartwig.pipeline.calling.command.BwaCommand;
import com.hartwig.pipeline.calling.command.SamtoolsCommand;
import com.hartwig.pipeline.calling.structural.gridss.stage.Driver;
import com.hartwig.pipeline.calling.structural.gridss.stage.Filter;
import com.hartwig.pipeline.calling.structural.gridss.stage.TabixDriverOutput;
import com.hartwig.pipeline.calling.structural.gridss.stage.ViralAnnotation;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.execution.vm.unix.ExportPathCommand;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.report.EntireOutputComponent;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.report.RunLogComponent;
import com.hartwig.pipeline.report.StartupScriptComponent;
import com.hartwig.pipeline.report.ZippedVcfAndIndexComponent;
import com.hartwig.pipeline.resource.RefGenomeVersion;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;

public class StructuralCaller implements Stage<StructuralCallerOutput, SomaticRunMetadata> {
    public static final String NAMESPACE = "structural_caller";

    private final InputDownload referenceBam;
    private final InputDownload referenceBai;
    private final InputDownload tumorBam;
    private final InputDownload tumorBai;

    private final ResourceFiles resourceFiles;
    private String unfilteredVcf;
    private String somaticFilteredVcf;
    private String somaticAndQualityFilteredVcf;

    public StructuralCaller(final AlignmentPair pair, final ResourceFiles resourceFiles) {
        this.resourceFiles = resourceFiles;
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
        commands.add(new ExportPathCommand(new BwaCommand()));
        commands.add(new ExportPathCommand(new SamtoolsCommand()));

        // TEMP
        if (resourceFiles.version() == RefGenomeVersion.HG38) {
            final String bwtFileOld = "/opt/resources/reference_genome/hg38/Homo_sapiens_assembly38.fasta.64.bwt";
            final String bwtFileNew = "/opt/resources/reference_genome/hg38/Homo_sapiens_assembly38.fasta.bwt";

            if (Files.exists(Paths.get(bwtFileOld)) && !Files.exists(Paths.get(bwtFileNew))) {
                commands.add(() -> format("cp %s %s", bwtFileOld, bwtFileNew));
            }
        }

        String tumorSampleName = metadata.tumor().sampleName();
        String refBamPath = referenceBam.getLocalTargetPath();
        String tumorBamPath = tumorBam.getLocalTargetPath();

        String configurationFilePath = ResourceFiles.of(GRIDSS_CONFIG, "gridss.properties");
        String blacklistBedPath = resourceFiles.gridssBlacklistBed();
        String virusReferenceGenomePath = ResourceFiles.of(VIRUS_REFERENCE_GENOME, "human_virus.fa");

        Driver driver = new Driver(VmDirectories.outputFile(tumorSampleName + ".assembly.bam"),
                resourceFiles.refGenomeFile(),
                blacklistBedPath,
                configurationFilePath,
                resourceFiles.gridssRepeatMaskerDbBed(),
                refBamPath,
                tumorBamPath);
        SubStageInputOutput unfilteredVcfOutput = driver.andThen(new TabixDriverOutput()).apply(SubStageInputOutput.empty(tumorSampleName));

        SubStageInputOutput unfilteredAnnotatedVcfOutput = new ViralAnnotation(virusReferenceGenomePath).apply(unfilteredVcfOutput);

        String somaticFilteredVcfBasename = VmDirectories.outputFile(format("%s.gridss.somatic.vcf", tumorSampleName));
        String somaticAndQualityFilteredVcfBasename = VmDirectories.outputFile(format("%s.gridss.somatic.filtered.vcf", tumorSampleName));
        SubStageInputOutput filteredAndAnnotated =
                new Filter(somaticAndQualityFilteredVcfBasename, somaticFilteredVcfBasename).apply(unfilteredAnnotatedVcfOutput);
        commands.addAll(filteredAndAnnotated.bash());

        unfilteredVcf = unfilteredVcfOutput.outputFile().path();
        somaticFilteredVcf = somaticFilteredVcfBasename + ".gz";
        somaticAndQualityFilteredVcf = somaticAndQualityFilteredVcfBasename + ".gz";
        return commands;
    }

    private static String basename(String filename) {
        return new File(filename).getName();
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
                .maybeFilteredVcf(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(basename(somaticAndQualityFilteredVcf))))
                .maybeFilteredVcfIndex(GoogleStorageLocation.of(bucket.name(),
                        resultsDirectory.path(basename(somaticAndQualityFilteredVcf + ".tbi"))))
                .maybeFullVcf(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(basename(somaticFilteredVcf))))
                .maybeFullVcfIndex(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(basename(somaticFilteredVcf + ".tbi"))))
                .addReportComponents(new ZippedVcfAndIndexComponent(bucket,
                        NAMESPACE,
                        Folder.from(),
                        basename(unfilteredVcf),
                        basename(unfilteredVcf),
                        resultsDirectory))
                .addReportComponents(new ZippedVcfAndIndexComponent(bucket,
                        NAMESPACE,
                        Folder.from(),
                        basename(somaticFilteredVcf),
                        basename(somaticFilteredVcf),
                        resultsDirectory))
                .addReportComponents(new ZippedVcfAndIndexComponent(bucket,
                        NAMESPACE,
                        Folder.from(),
                        basename(somaticAndQualityFilteredVcf),
                        basename(somaticAndQualityFilteredVcf),
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
