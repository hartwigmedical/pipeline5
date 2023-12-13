package com.hartwig.pipeline.tertiary.peach;

import static com.hartwig.computeengine.execution.vm.command.InputDownloadCommand.initialiseOptionalLocation;
import static com.hartwig.pipeline.tools.HmfTool.PEACH;

import java.util.List;

import com.hartwig.computeengine.execution.vm.VirtualMachinePerformanceProfile;
import com.hartwig.computeengine.execution.vm.command.InputDownloadCommand;
import com.hartwig.computeengine.execution.vm.command.python.Python3Command;
import com.hartwig.pipeline.Arguments;
import com.hartwig.computeengine.storage.ResultsDirectory;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.PipelineStatus;
import com.hartwig.computeengine.execution.vm.command.BashCommand;
import com.hartwig.computeengine.execution.vm.BashStartupScript;
import com.hartwig.computeengine.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.computeengine.execution.vm.VmDirectories;
import com.hartwig.pipeline.input.SomaticRunMetadata;
import com.hartwig.pipeline.output.AddDatatype;
import com.hartwig.pipeline.output.ArchivePath;
import com.hartwig.pipeline.output.EntireOutputComponent;
import com.hartwig.pipeline.output.Folder;
import com.hartwig.pipeline.output.RunLogComponent;
import com.hartwig.pipeline.reruns.PersistedDataset;
import com.hartwig.pipeline.reruns.PersistedLocations;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.Namespace;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.computeengine.storage.RuntimeBucket;
import com.hartwig.pipeline.tertiary.purple.PurpleOutput;

import org.jetbrains.annotations.NotNull;

@Namespace(Peach.NAMESPACE)
public class Peach implements Stage<PeachOutput, SomaticRunMetadata> {
    public static final String NAMESPACE = "peach";
    public static final String PEACH_GENOTYPE_TSV = ".peach.genotype.tsv";
    private static final String PEACH_CALLS_TSV = ".peach.calls.tsv";
    private final InputDownloadCommand purpleGermlineVariantsDownload;
    private final ResourceFiles resourceFiles;
    private final PersistedDataset persistedDataset;

    public Peach(final PurpleOutput purpleOutput, final ResourceFiles resourceFiles, final PersistedDataset persistedDataset) {
        purpleGermlineVariantsDownload = initialiseOptionalLocation(purpleOutput.outputLocations().germlineVariants());
        this.resourceFiles = resourceFiles;
        this.persistedDataset = persistedDataset;
    }

    @Override
    public List<BashCommand> inputs() {
        return List.of(purpleGermlineVariantsDownload);
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> referenceOnlyCommands(final SomaticRunMetadata metadata) {
        return peachCommands(metadata.reference().sampleName(), metadata.reference().sampleName());
    }

    @Override
    public List<BashCommand> tumorReferenceCommands(final SomaticRunMetadata metadata) {
        return peachCommands(metadata.tumor().sampleName(), metadata.reference().sampleName());
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.builder()
                .imageFamily(IMAGE_FAMILY)
                .name(NAMESPACE)
                .startupCommand(bash)
                .namespacedResults(resultsDirectory)
                .performanceProfile(VirtualMachinePerformanceProfile.custom(PEACH.getCpus(), PEACH.getMemoryGb()))
                .workingDiskSpaceGb(375)
                .build();
    }

    @Override
    public PeachOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        return PeachOutput.builder()
                .status(jobStatus)
                .maybeGenotypes(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(genotypeTsv(metadata.sampleName()))))
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .addReportComponents(new EntireOutputComponent(bucket, Folder.root(), namespace(), resultsDirectory))
                .addAllDatatypes(addDatatypes(metadata))
                .build();
    }

    @Override
    public PeachOutput skippedOutput(final SomaticRunMetadata metadata) {
        return PeachOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public PeachOutput persistedOutput(final SomaticRunMetadata metadata) {
        String genotypeTsv = genotypeTsv(metadata.sampleName());
        return PeachOutput.builder()
                .status(PipelineStatus.PERSISTED)
                .maybeGenotypes(persistedDataset.path(metadata.sampleName(), DataType.PEACH_GENOTYPE)
                        .orElse(GoogleStorageLocation.of(metadata.bucket(),
                                PersistedLocations.blobForSet(metadata.set(), namespace(), genotypeTsv))))
                .addAllDatatypes(addDatatypes(metadata))
                .build();
    }

    @Override
    public List<AddDatatype> addDatatypes(final SomaticRunMetadata metadata) {
        return List.of(new AddDatatype(DataType.PEACH_CALLS,
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), metadata.sampleName() + PEACH_CALLS_TSV)),
                new AddDatatype(DataType.PEACH_GENOTYPE,
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), genotypeTsv(metadata.sampleName()))));
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return !arguments.shallow() && arguments.runTertiary();
    }

    public List<BashCommand> peachCommands(final String filenameId, final String referenceId) {
        return List.of(new Python3Command(PEACH.getToolName(),
                PEACH.runVersion(),
                "src/main.py",
                List.of("--vcf",
                        purpleGermlineVariantsDownload.getLocalTargetPath(),
                        "--sample_t_id",
                        filenameId,
                        "--sample_r_id",
                        referenceId,
                        "--tool_version",
                        PEACH.runVersion(),
                        "--outputdir",
                        VmDirectories.OUTPUT,
                        "--panel",
                        resourceFiles.peachFilterBed())));
    }

    @NotNull
    protected String genotypeTsv(final String sampleName) {
        return sampleName + PEACH_GENOTYPE_TSV;
    }
}
