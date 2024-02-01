package com.hartwig.pipeline.tertiary.cobalt;

import com.google.api.client.util.Lists;
import com.hartwig.computeengine.execution.vm.Bash;
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
import com.hartwig.pipeline.output.*;
import com.hartwig.pipeline.reruns.PersistedDataset;
import com.hartwig.pipeline.reruns.PersistedLocations;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.Namespace;
import com.hartwig.pipeline.tertiary.TertiaryStage;

import java.util.List;

import static com.hartwig.pipeline.tools.HmfTool.COBALT;

@Namespace(Cobalt.NAMESPACE)
public class Cobalt extends TertiaryStage<CobaltOutput> {

    public static final String NAMESPACE = "cobalt";

    private final ResourceFiles resourceFiles;
    private final PersistedDataset persistedDataset;
    private final Arguments arguments;

    public Cobalt(final AlignmentPair alignmentPair, final ResourceFiles resourceFiles, final PersistedDataset persistedDataset,
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

        arguments.add(String.format("-tumor_only_diploid_bed %s", resourceFiles.diploidRegionsBed()));

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
        List<BashCommand> commands = Lists.newArrayList(List.of(
                () -> "eval `/root/anaconda3/bin/conda shell.bash hook`",
                () -> "source /root/anaconda3/bin/activate",
                () -> "conda activate /root/anaconda3/envs/bioconductor-r42"));
        commands.add(JavaCommandFactory.javaJarCommand(COBALT, arguments));
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
        arguments.add(String.format("-gc_profile %s", resourceFiles.gcProfileFile()));
        arguments.add(String.format("-output_dir %s", VmDirectories.OUTPUT));
        arguments.add(String.format("-threads %s", Bash.allCpus()));
    }

    private void addTargetRegionsArguments(final List<String> cobaltArguments) {
        if (arguments.useTargetRegions()) {
            cobaltArguments.add(String.format("-target_region %s", resourceFiles.targetRegionsNormalisation()));
            cobaltArguments.add("-pcf_gamma 50");
        }
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinitions.cobalt(bash, resultsDirectory);
    }

    @Override
    public CobaltOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        return CobaltOutput.builder()
                .status(jobStatus)
                .maybeOutputDirectory(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(), true))
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .addReportComponents(new EntireOutputComponent(bucket, Folder.root(), NAMESPACE, resultsDirectory))
                .addAllDatatypes(addDatatypes(metadata))
                .build();
    }

    @Override
    public CobaltOutput skippedOutput(final SomaticRunMetadata metadata) {
        return CobaltOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public CobaltOutput persistedOutput(final SomaticRunMetadata metadata) {
        return CobaltOutput.builder()
                .status(PipelineStatus.PERSISTED)
                .maybeOutputDirectory(persistedDataset.path(metadata.tumor().sampleName(), DataType.COBALT)
                        .map(GoogleStorageLocation::asDirectory)
                        .orElse(GoogleStorageLocation.of(metadata.bucket(),
                                PersistedLocations.pathForSet(metadata.set(), namespace()),
                                true)))
                .addAllDatatypes(addDatatypes(metadata))
                .build();
    }

    @Override
    public List<AddDatatype> addDatatypes(final SomaticRunMetadata metadata) {
        return List.of(new AddDatatype(DataType.COBALT,
                metadata.barcode(),
                new ArchivePath(Folder.root(), namespace(), outputFilename(metadata)),
                true));
    }

    private String outputFilename(final SomaticRunMetadata metadata) {
        return metadata.sampleName() + ".cobalt.ratio.tsv.gz";
    }
}