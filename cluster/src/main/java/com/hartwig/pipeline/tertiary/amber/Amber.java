package com.hartwig.pipeline.tertiary.amber;

import static com.hartwig.pipeline.tools.HmfTool.AMBER;

import java.util.List;

import com.google.api.client.util.Lists;
import com.hartwig.computeengine.execution.vm.BashStartupScript;
import com.hartwig.computeengine.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.computeengine.execution.vm.VmDirectories;
import com.hartwig.computeengine.execution.vm.command.BashCommand;
import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.computeengine.storage.ResultsDirectory;
import com.hartwig.computeengine.storage.RuntimeBucket;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.PipelineStatus;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.execution.JavaCommandFactory;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinitions;
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
import com.hartwig.pipeline.tertiary.TertiaryStage;

@Namespace(Amber.NAMESPACE)
public class Amber extends TertiaryStage<AmberOutput> {

    public static final String NAMESPACE = "amber";

    private final ResourceFiles resourceFiles;
    private final PersistedDataset persistedDataset;
    private final Arguments arguments;

    public Amber(final AlignmentPair alignmentPair, final ResourceFiles resourceFiles, final PersistedDataset persistedDataset,
            final Arguments arguments) {
        super(alignmentPair);
        this.resourceFiles = resourceFiles;
        this.persistedDataset = persistedDataset;
        this.arguments = arguments;
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> tumorReferenceCommands(final SomaticRunMetadata metadata) {

        List<String> arguments = Lists.newArrayList();

        addTumor(arguments, metadata);
        addReference(arguments, metadata);
        addCommonArguments(arguments);
        addTargetRegionsArguments(arguments);

        return formCommand(arguments);
    }

    @Override
    public List<BashCommand> tumorOnlyCommands(final SomaticRunMetadata metadata) {
        List<String> arguments = Lists.newArrayList();

        addTumor(arguments, metadata);
        addCommonArguments(arguments);
        addTargetRegionsArguments(arguments);

        return formCommand(arguments);
    }

    @Override
    public List<BashCommand> referenceOnlyCommands(final SomaticRunMetadata metadata) {
        List<String> arguments = Lists.newArrayList();

        addReference(arguments, metadata);
        addCommonArguments(arguments);

        return formCommand(arguments);
    }

    private List<BashCommand> formCommand(final List<String> arguments) {
        List<BashCommand> commands = Lists.newArrayList();
        commands.add(JavaCommandFactory.javaJarCommand(AMBER, arguments));
        return commands;
    }

    private void addTumor(final List<String> arguments, final SomaticRunMetadata metadata) {
        arguments.add(String.format("-tumor %s", metadata.tumor().sampleName()));
        arguments.add(String.format("-tumor_bam %s", getTumorBamDownload().getLocalTargetPath()));
    }

    private void addReference(final List<String> arguments, final SomaticRunMetadata metadata) {
        arguments.add(String.format("-reference %s", metadata.reference().sampleName()));
        arguments.add(String.format("-reference_bam %s", getReferenceBamDownload().getLocalTargetPath()));
    }

    private void addCommonArguments(final List<String> arguments) {
        arguments.add(String.format("-ref_genome %s", resourceFiles.refGenomeFile()));
        arguments.add(String.format("-ref_genome_version %s", resourceFiles.version()));
        arguments.add(String.format("-loci %s", resourceFiles.amberHeterozygousLoci()));
        arguments.add(String.format("-output_dir %s", VmDirectories.OUTPUT));
        arguments.add(String.format("-threads %s", "12"));
    }

    private void addTargetRegionsArguments(final List<String> amberArguments) {
        if (arguments.useTargetRegions()) {
            amberArguments.add("-tumor_only_min_depth 80");
        }
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinitions.amber(bash, resultsDirectory);
    }

    @Override
    public AmberOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        return AmberOutput.builder()
                .status(jobStatus)
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .maybeOutputDirectory(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(), true))
                .addReportComponents(new EntireOutputComponent(bucket, Folder.root(), namespace(), resultsDirectory))
                .addAllDatatypes(addDatatypes(metadata))
                .build();
    }

    @Override
    public AmberOutput skippedOutput(final SomaticRunMetadata metadata) {
        return AmberOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public AmberOutput persistedOutput(final SomaticRunMetadata metadata) {
        return AmberOutput.builder()
                .status(PipelineStatus.PERSISTED)
                .maybeOutputDirectory(persistedDataset.path(metadata.tumor().sampleName(), DataType.AMBER)
                        .map(GoogleStorageLocation::asDirectory)
                        .orElse(GoogleStorageLocation.of(metadata.bucket(),
                                PersistedLocations.pathForSet(metadata.set(), namespace()),
                                true)))
                .addAllDatatypes(addDatatypes(metadata))
                .build();
    }

    @Override
    public List<AddDatatype> addDatatypes(final SomaticRunMetadata metadata) {
        return List.of(new AddDatatype(DataType.AMBER,
                metadata.barcode(),
                new ArchivePath(Folder.root(), namespace(), String.format("%s.amber.baf.tsv.gz", metadata.sampleName())),
                true));
    }

}
