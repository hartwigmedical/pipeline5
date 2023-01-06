package com.hartwig.pipeline.tertiary.sigs;

import static com.hartwig.pipeline.execution.vm.InputDownload.initialiseOptionalLocation;

import java.util.List;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VirtualMachinePerformanceProfile;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.execution.vm.java.JavaJarCommand;
import com.hartwig.pipeline.output.AddDatatype;
import com.hartwig.pipeline.output.ArchivePath;
import com.hartwig.pipeline.input.SomaticRunMetadata;
import com.hartwig.pipeline.report.EntireOutputComponent;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.report.RunLogComponent;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.Namespace;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tertiary.purple.PurpleOutput;
import com.hartwig.pipeline.tools.Versions;

@Namespace(Sigs.NAMESPACE)
public class Sigs implements Stage<SigsOutput, SomaticRunMetadata> {
    public static final String NAMESPACE = "sigs";

    private final InputDownload purpleSomaticVariantsDownload;

    private final ResourceFiles resourceFiles;

    public Sigs(final PurpleOutput purpleOutput, final ResourceFiles resourceFiles) {
        purpleSomaticVariantsDownload = initialiseOptionalLocation(purpleOutput.outputLocations().somaticVariants());
        this.resourceFiles = resourceFiles;
    }

    @Override
    public List<BashCommand> inputs() {
        return List.of(purpleSomaticVariantsDownload);
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> tumorReferenceCommands(final SomaticRunMetadata metadata) {
        return buildCommands(metadata);
    }

    @Override
    public List<BashCommand> tumorOnlyCommands(final SomaticRunMetadata metadata) {
        return buildCommands(metadata);
    }

    @Override
    public List<BashCommand> referenceOnlyCommands(final SomaticRunMetadata metadata) {
        return Stage.disabled();
    }

    private List<BashCommand> buildCommands(final SomaticRunMetadata metadata) {
        return List.of(new JavaJarCommand("sigs",
                Versions.SIGS,
                "sigs.jar",
                "4G",
                List.of("-sample",
                        metadata.tumor().sampleName(),
                        "-signatures_file",
                        resourceFiles.snvSignatures(),
                        "-somatic_vcf_file",
                        purpleSomaticVariantsDownload.getLocalTargetPath(),
                        "-output_dir",
                        VmDirectories.OUTPUT)));
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.builder()
                .name(NAMESPACE)
                .startupCommand(bash)
                .namespacedResults(resultsDirectory)
                .performanceProfile(VirtualMachinePerformanceProfile.custom(4, 16))
                .workingDiskSpaceGb(375).build();
    }

    @Override
    public SigsOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        return SigsOutput.builder()
                .status(jobStatus)
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .addReportComponents(new EntireOutputComponent(bucket, Folder.root(), namespace(), resultsDirectory))
                .addAllDatatypes(addDatatypes(metadata))
                .build();
    }

    @Override
    public List<AddDatatype> addDatatypes(final SomaticRunMetadata metadata) {
        return List.of(new AddDatatype(DataType.SIGNATURE_ALLOCATION,
                metadata.barcode(),
                new ArchivePath(Folder.root(), namespace(), metadata.tumor().sampleName() + ".sig.allocation.tsv")));
    }

    @Override
    public SigsOutput skippedOutput(final SomaticRunMetadata metadata) {
        return SigsOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public SigsOutput persistedOutput(final SomaticRunMetadata metadata) {
        return SigsOutput.builder().status(PipelineStatus.PERSISTED).addAllDatatypes(addDatatypes(metadata)).build();
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return !arguments.shallow() && arguments.runTertiary() && !arguments.useTargetRegions();
    }
}
