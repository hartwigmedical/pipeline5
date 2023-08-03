package com.hartwig.pipeline.tertiary.cider;

import static com.hartwig.pipeline.execution.vm.VirtualMachinePerformanceProfile.custom;
import static com.hartwig.pipeline.tools.ExternalTool.BLASTN;

import java.util.ArrayList;
import java.util.List;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.Bash;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.ImmutableVirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.execution.vm.command.java.JavaJarCommand;
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
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tertiary.TertiaryStage;
import com.hartwig.pipeline.tools.HmfTool;

@Namespace(Cider.NAMESPACE)
public class Cider extends TertiaryStage<CiderOutput> {

    public static final String NAMESPACE = "cider";

    private final ResourceFiles resourceFiles;
    private final PersistedDataset persistedDataset;

    public Cider(final AlignmentPair alignmentPair, final ResourceFiles resourceFiles, final PersistedDataset persistedDataset) {
        super(alignmentPair);
        this.resourceFiles = resourceFiles;
        this.persistedDataset = persistedDataset;
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> tumorReferenceCommands(final SomaticRunMetadata metadata) {
        return tumorOnlyCommands(metadata);
    }

    @Override
    public List<BashCommand> tumorOnlyCommands(final SomaticRunMetadata metadata) {
        List<String> arguments = ciderArguments(metadata);
        return List.of(new JavaJarCommand(HmfTool.CIDER, arguments));
    }

    private List<String> ciderArguments(final SomaticRunMetadata metadata) {
        List<String> arguments = new ArrayList<>();
        arguments.add(String.format("-sample %s", metadata.tumor().sampleName()));
        arguments.add(String.format("-bam %s", getTumorBamDownload().getLocalTargetPath()));
        arguments.add(String.format("-ref_genome %s", resourceFiles.refGenomeFile()));
        arguments.add(String.format("-ref_genome_version %s", resourceFiles.version()));
        arguments.add(String.format("-blast %s", BLASTN.path()));
        arguments.add(String.format("-blast_db %s", resourceFiles.blastDb()));
        arguments.add("-write_cider_bam");
        arguments.add(String.format("-output_dir %s", VmDirectories.OUTPUT));
        arguments.add(String.format("-threads %s", Bash.allCpus()));
        return arguments;
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript startupScript, final ResultsDirectory resultsDirectory) {
        return ImmutableVirtualMachineJobDefinition.builder()
                .name("cider")
                .startupCommand(startupScript)
                .performanceProfile(custom(HmfTool.CIDER.getCpus(), HmfTool.CIDER.getMemoryGb()))
                .namespacedResults(resultsDirectory)
                .build();
    }

    @Override
    public CiderOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        ImmutableCiderOutputLocations.Builder outputLocationsBuilder = CiderOutputLocations.builder();

        metadata.maybeTumor().ifPresent(tumor -> {
            final String tumorSampleName = tumor.sampleName();
            String vdj = vdj(tumorSampleName);
            String locusStats = locusStats(tumorSampleName);
            String layouts = layouts(tumorSampleName);
            String ciderBam = ciderBam(tumorSampleName);

            outputLocationsBuilder.vdj(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(vdj)))
                    .locusStats(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(locusStats)))
                    .layouts(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(layouts)))
                    .ciderBam(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(ciderBam)));
        });

        return CiderOutput.builder()
                .status(jobStatus)
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .addReportComponents(new EntireOutputComponent(bucket, Folder.root(), namespace(), resultsDirectory))
                .maybeOutputLocations(outputLocationsBuilder.build())
                .addAllDatatypes(addDatatypes(metadata))
                .build();
    }

    @Override
    public CiderOutput skippedOutput(final SomaticRunMetadata metadata) {
        return CiderOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public CiderOutput persistedOutput(final SomaticRunMetadata metadata) {

        final String tumorSampleName = metadata.tumor().sampleName();
        String vdj = vdj(tumorSampleName);
        String locusStats = locusStats(tumorSampleName);
        String layouts = layouts(tumorSampleName);
        String ciderBam = ciderBam(tumorSampleName);

        GoogleStorageLocation vdjLocation = persistedOrDefault(metadata.sampleName(),
                metadata.set(),
                metadata.bucket(),
                DataType.CIDER_VDJ,
                vdj);
        GoogleStorageLocation locusStatsLocation =persistedOrDefault(metadata.sampleName(),
                metadata.set(),
                metadata.bucket(),
                DataType.CIDER_LOCUS_STATS,
                locusStats);
        GoogleStorageLocation layoutsLocation = persistedOrDefault(metadata.sampleName(),
                metadata.set(),
                metadata.bucket(),
                DataType.CIDER_LAYOUTS,
                layouts);
        GoogleStorageLocation ciderBamLocation = persistedOrDefault(metadata.sampleName(),
                metadata.set(),
                metadata.bucket(),
                DataType.CIDER_BAM,
                ciderBam);

        ImmutableCiderOutputLocations.Builder outputLocationsBuilder = CiderOutputLocations.builder()
                .vdj(vdjLocation)
                .locusStats(locusStatsLocation)
                .layouts(layoutsLocation)
                .ciderBam(ciderBamLocation);

        return CiderOutput.builder()
                .status(PipelineStatus.PERSISTED)
                .addAllDatatypes(addDatatypes(metadata))
                .maybeOutputLocations(outputLocationsBuilder.build())
                .build();
    }

    @Override
    public List<AddDatatype> addDatatypes(final SomaticRunMetadata metadata)
    {
        List<AddDatatype> datatypes = new ArrayList<>();
        metadata.maybeTumor().ifPresent(tumor ->
        {
            final String tumorSampleName = tumor.sampleName();
            String vdj = vdj(tumorSampleName);
            String locusStats = locusStats(tumorSampleName);
            String layouts = layouts(tumorSampleName);
            String ciderBam = ciderBam(tumorSampleName);

            datatypes.add(new AddDatatype(DataType.CIDER_VDJ, metadata.barcode(), new ArchivePath(Folder.root(), namespace(), vdj)));
            datatypes.add(new AddDatatype(DataType.CIDER_LOCUS_STATS, metadata.barcode(), new ArchivePath(Folder.root(), namespace(), locusStats)));
            datatypes.add(new AddDatatype(DataType.CIDER_LAYOUTS, metadata.barcode(), new ArchivePath(Folder.root(), namespace(), layouts)));
            datatypes.add(new AddDatatype(DataType.CIDER_BAM, metadata.barcode(), new ArchivePath(Folder.root(), namespace(), ciderBam)));
        });

        return datatypes;
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runTertiary();
    }

    private GoogleStorageLocation persistedOrDefault(final String sample, final String set, final String bucket,
            final DataType dataType, final String fileName) {
        return persistedDataset.path(sample, dataType)
                .orElse(GoogleStorageLocation.of(bucket, PersistedLocations.blobForSet(set, namespace(), fileName)));
    }

    private static String vdj(final String sample) {
        return sample + ".cider.vdj.tsv.gz";
    }
    private static String locusStats(final String sample) {
        return sample + ".cider.locus_stats.tsv";
    }
    private static String layouts(final String sample) {
        return sample + ".cider.layout.gz";
    }
    private static String ciderBam(final String sample) {
        return sample + ".cider.bam";
    }
}