package com.hartwig.pipeline.tertiary.lilac;

import static com.hartwig.pipeline.execution.vm.InputDownload.initialiseOptionalLocation;

import java.util.ArrayList;
import java.util.List;

import com.google.api.client.util.Lists;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.Bash;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.ImmutableVirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.SambambaCommand;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VirtualMachinePerformanceProfile;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.execution.vm.java.JavaJarCommand;
import com.hartwig.pipeline.metadata.AddDatatype;
import com.hartwig.pipeline.metadata.ArchivePath;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.report.EntireOutputComponent;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.report.RunLogComponent;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tertiary.TertiaryStage;
import com.hartwig.pipeline.tertiary.purple.PurpleOutput;
import com.hartwig.pipeline.tertiary.purple.PurpleOutputLocations;
import com.hartwig.pipeline.tools.Versions;

public class Lilac extends TertiaryStage<LilacOutput> {
    public static final String NAMESPACE = "lilac";

    private final ResourceFiles resourceFiles;
    private final InputDownload purpleGeneCopyNumber;
    private final InputDownload purpleSomaticVariants;

    public Lilac(final AlignmentPair alignmentPair, final ResourceFiles resourceFiles, final PurpleOutput purpleOutput) {
        super(alignmentPair);
        this.resourceFiles = resourceFiles;
        PurpleOutputLocations purpleOutputLocations = purpleOutput.outputLocations();
        this.purpleGeneCopyNumber = initialiseOptionalLocation(purpleOutputLocations.geneCopyNumber());
        this.purpleSomaticVariants = initialiseOptionalLocation(purpleOutputLocations.somaticVariants());
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> inputs() {
        List<BashCommand> result = new ArrayList<>(super.inputs());
        result.add(purpleGeneCopyNumber);
        result.add(purpleSomaticVariants);
        return result;
    }

    @Override
    public List<BashCommand> tumorReferenceCommands(final SomaticRunMetadata metadata)
    {
        String slicedRefBam = VmDirectories.outputFile(metadata.reference().sampleName() + ".hla.bam");
        String slicedTumorBam = VmDirectories.outputFile(metadata.tumor().sampleName() + ".hla.bam");

        List<BashCommand> commands = Lists.newArrayList();

        addSliceCommands(commands, metadata.reference().sampleName(), getReferenceBamDownload().getLocalTargetPath(), slicedRefBam);
        addSliceCommands(commands, metadata.tumor().sampleName(), getTumorBamDownload().getLocalTargetPath(), slicedTumorBam);

        List<String> arguments = Lists.newArrayList();
        arguments.addAll(commonArguments(metadata.tumor().sampleName(), slicedRefBam));

        arguments.add(String.format("-tumor_bam %s", slicedTumorBam));
        arguments.add(String.format("-gene_copy_number_file %s", purpleGeneCopyNumber.getLocalTargetPath()));
        arguments.add(String.format("-somatic_variants_file %s", purpleSomaticVariants.getLocalTargetPath()));

        commands.add(formCommand(arguments));

        return commands;
    }

    @Override
    public List<BashCommand> referenceOnlyCommands(final SomaticRunMetadata metadata)
    {
        String slicedRefBam = VmDirectories.outputFile(metadata.reference().sampleName() + ".hla.bam");

        List<BashCommand> commands = Lists.newArrayList();

        addSliceCommands(commands, metadata.reference().sampleName(), getReferenceBamDownload().getLocalTargetPath(), slicedRefBam);

        List<String> arguments = Lists.newArrayList();
        arguments.addAll(commonArguments(metadata.reference().sampleName(), slicedRefBam));
        commands.add(formCommand(arguments));

        return commands;
    }

    @Override
    public List<BashCommand> tumorOnlyCommands(final SomaticRunMetadata metadata)
    {
        String slicedTumorBam = VmDirectories.outputFile(metadata.tumor().sampleName() + ".hla.bam");

        List<BashCommand> commands = Lists.newArrayList();

        addSliceCommands(commands, metadata.tumor().sampleName(), getTumorBamDownload().getLocalTargetPath(), slicedTumorBam);

        List<String> arguments = Lists.newArrayList();
        arguments.addAll(commonArguments(metadata.tumor().sampleName(), slicedTumorBam));
        commands.add(formCommand(arguments));

        return commands;
    }

    private List<String> commonArguments(final String sampleName, final String bamFile)
    {
        List<String> arguments = Lists.newArrayList();

        arguments.add(String.format("-sample %s", sampleName));
        arguments.add(String.format("-reference_bam %s", bamFile));
        arguments.add(String.format("-ref_genome %s", resourceFiles.refGenomeFile()));
        arguments.add(String.format("-ref_genome_version %s", resourceFiles.version()));
        arguments.add(String.format("-resource_dir %s", resourceFiles.lilacResources()));
        arguments.add(String.format("-output_dir %s", VmDirectories.OUTPUT));
        arguments.add(String.format("-threads %s", Bash.allCpus()));

        return arguments;
    }

    private JavaJarCommand formCommand(final List<String> arguments)
    {
        return new JavaJarCommand("lilac", Versions.LILAC, "lilac.jar", "15G", arguments);
    }

    private void addSliceCommands(
            final List<BashCommand> commands, final String sampleName, final String inputBamFile, final String slicedBamFile)
    {
        commands.add(new SambambaCommand("slice", "-L", resourceFiles.hlaRegionBed(), "-o", slicedBamFile, inputBamFile));
        commands.add(new SambambaCommand("index", slicedBamFile));
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return ImmutableVirtualMachineJobDefinition.builder()
                .name("lilac")
                .startupCommand(bash)
                .performanceProfile(VirtualMachinePerformanceProfile.custom(8, 16))
                .namespacedResults(resultsDirectory)
                .build();
    }

    @Override
    public LilacOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        return LilacOutput.builder()
                .status(jobStatus)
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .addReportComponents(new EntireOutputComponent(bucket, Folder.root(), namespace(), resultsDirectory))
                .addDatatypes(new AddDatatype(DataType.LILAC_OUTPUT,
                                metadata.barcode(),
                                new ArchivePath(Folder.root(), namespace(), metadata.tumor().sampleName() + ".lilac.csv")),
                        new AddDatatype(DataType.LILAC_QC_METRICS,
                                metadata.barcode(),
                                new ArchivePath(Folder.root(), namespace(), metadata.tumor().sampleName() + ".lilac.qc.csv")))
                .build();
    }

    @Override
    public LilacOutput skippedOutput(final SomaticRunMetadata metadata) {
        return LilacOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runTertiary() && !arguments.shallow();
    }
}
