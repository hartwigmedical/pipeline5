package com.hartwig.pipeline.calling.structural.gripss;

import static com.hartwig.pipeline.tools.HmfTool.GRIPSS;

import java.io.File;
import java.util.List;

import com.google.api.client.util.Lists;
import com.google.common.collect.ImmutableList;
import com.hartwig.computeengine.execution.vm.BashStartupScript;
import com.hartwig.computeengine.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.computeengine.execution.vm.VmDirectories;
import com.hartwig.computeengine.execution.vm.command.BashCommand;
import com.hartwig.computeengine.execution.vm.command.InputDownloadCommand;
import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.computeengine.storage.ResultsDirectory;
import com.hartwig.computeengine.storage.RuntimeBucket;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.PipelineStatus;
import com.hartwig.pipeline.calling.structural.gridss.GridssOutput;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.JavaCommandFactory;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinitions;
import com.hartwig.pipeline.input.SomaticRunMetadata;
import com.hartwig.pipeline.output.AddDatatype;
import com.hartwig.pipeline.output.ArchivePath;
import com.hartwig.pipeline.output.Folder;
import com.hartwig.pipeline.output.RunLogComponent;
import com.hartwig.pipeline.output.StartupScriptComponent;
import com.hartwig.pipeline.output.ZippedVcfAndIndexComponent;
import com.hartwig.pipeline.reruns.PersistedDataset;
import com.hartwig.pipeline.reruns.PersistedLocations;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.Stage;

public abstract class Gripss implements Stage<GripssOutput, SomaticRunMetadata> {

    private final InputDownloadCommand gridssVcf;
    private final InputDownloadCommand gridssVcfIndex;
    protected final ResourceFiles resourceFiles;

    private final PersistedDataset persistedDataset;
    private final String namespace;

    public Gripss(final GridssOutput gridssOutput, final PersistedDataset persistedDataset, final ResourceFiles resourceFiles,
            final String namespace) {

        this.resourceFiles = resourceFiles;
        this.gridssVcf = new InputDownloadCommand(gridssOutput.unfilteredVariants());
        this.gridssVcfIndex = new InputDownloadCommand(gridssOutput.unfilteredVariants().transform(FileTypes::tabixIndex));
        this.persistedDataset = persistedDataset;
        this.namespace = namespace;
    }

    @Override
    public List<BashCommand> inputs() {
        return ImmutableList.of(gridssVcf, gridssVcfIndex);
    }

    @Override
    public String namespace() { return namespace; }

    protected List<BashCommand> formCommand(final List<String> arguments) {
        List<BashCommand> commands = Lists.newArrayList();
        commands.add(JavaCommandFactory.javaJarCommand(GRIPSS, arguments));
        return commands;
    }

    protected List<String> commonArguments() {

        List<String> arguments = Lists.newArrayList();

        arguments.add(String.format("-ref_genome %s", resourceFiles.refGenomeFile()));
        arguments.add(String.format("-ref_genome_version %s", resourceFiles.version().toString()));
        arguments.add(String.format("-known_hotspot_file %s", resourceFiles.knownFusionPairBedpe()));
        arguments.add(String.format("-pon_sgl_file %s", resourceFiles.sglBreakendPon()));
        arguments.add(String.format("-pon_sv_file %s", resourceFiles.svBreakpointPon()));
        arguments.add(String.format("-repeat_mask_file %s", resourceFiles.repeatMaskerDb()));
        arguments.add(String.format("-vcf %s", gridssVcf.getLocalTargetPath()));
        arguments.add(String.format("-output_dir %s", VmDirectories.OUTPUT));

        return arguments;
    }

    private static String basename(final String filename) {
        return new File(filename).getName();
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinitions.gripss(bash, resultsDirectory, namespace());
    }

    @Override
    public GripssOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {

        String filteredVcfFile = filteredVcf(metadata);
        String unfilteredVcfFile = unfilteredVcf(metadata);

        return GripssOutput.builder(namespace())
                .status(jobStatus)
                .maybeFilteredVariants(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(basename(filteredVcfFile))))
                .maybeUnfilteredVariants(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(basename(unfilteredVcfFile))))
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .addReportComponents(new ZippedVcfAndIndexComponent(bucket,
                        namespace(),
                        Folder.root(),
                        basename(unfilteredVcfFile), basename(unfilteredVcfFile), resultsDirectory))
                .addReportComponents(new ZippedVcfAndIndexComponent(bucket,
                        namespace(),
                        Folder.root(),
                        basename(filteredVcfFile),
                        basename(filteredVcfFile),
                        resultsDirectory))
                .addReportComponents(new RunLogComponent(bucket, namespace(), Folder.root(), resultsDirectory))
                .addReportComponents(new StartupScriptComponent(bucket, namespace(), Folder.root()))
                .addAllDatatypes(addDatatypes(metadata))
                .build();
    }

    @Override
    public List<AddDatatype> addDatatypes(final SomaticRunMetadata metadata) {
        return List.of(new AddDatatype(unfilteredDatatype(),
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), basename(unfilteredVcf(metadata)))),
                new AddDatatype(filteredDatatype(),
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), basename(filteredVcf(metadata)))));
    }

    @Override
    public GripssOutput skippedOutput(final SomaticRunMetadata metadata) {
        return GripssOutput.builder(namespace()).status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public GripssOutput persistedOutput(final SomaticRunMetadata metadata) {

        String filteredVcfFile = filteredVcf(metadata);
        String unfilteredVcfFile = unfilteredVcf(metadata);

        GoogleStorageLocation filteredLocation =
                persistedDataset.path(metadata.sampleName(), filteredDatatype())
                        .orElse(GoogleStorageLocation.of(metadata.bucket(),
                                PersistedLocations.blobForSet(metadata.set(), namespace(), filteredVcfFile)));
        GoogleStorageLocation unfilteredLocation =
                persistedDataset.path(metadata.sampleName(), unfilteredDatatype())
                        .orElse(GoogleStorageLocation.of(metadata.bucket(),
                                PersistedLocations.blobForSet(metadata.set(), namespace(), unfilteredVcfFile)));

        return GripssOutput.builder(namespace())
                .status(PipelineStatus.PERSISTED)
                .maybeFilteredVariants(filteredLocation)
                .maybeUnfilteredVariants(unfilteredLocation)
                .build();
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runTertiary();
    }

    public abstract String filteredVcf(final SomaticRunMetadata metadata);

    public abstract String unfilteredVcf(final SomaticRunMetadata metadata);

    public abstract DataType filteredDatatype();

    public abstract DataType unfilteredDatatype();
}
