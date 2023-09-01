package com.hartwig.pipeline.tertiary.lilac;

import static java.util.stream.Collectors.toList;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import com.hartwig.computeengine.execution.vm.Bash;
import com.hartwig.computeengine.execution.vm.BashStartupScript;
import com.hartwig.computeengine.execution.vm.ImmutableVirtualMachineJobDefinition;
import com.hartwig.computeengine.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.computeengine.execution.vm.VirtualMachinePerformanceProfile;
import com.hartwig.computeengine.execution.vm.VmDirectories;
import com.hartwig.computeengine.execution.vm.command.BashCommand;
import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.computeengine.storage.ResultsDirectory;
import com.hartwig.computeengine.storage.RuntimeBucket;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.PipelineStatus;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.alignment.bwa.SambambaCommand;
import com.hartwig.pipeline.calling.command.SamtoolsCommand;
import com.hartwig.pipeline.datatypes.DataType;
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
import com.hartwig.pipeline.tertiary.lilac.ImmutableLilacBamSliceOutput.Builder;

@Namespace(LilacBamSlicer.NAMESPACE)
public class LilacBamSlicer extends TertiaryStage<LilacBamSliceOutput> {
    public static final String NAMESPACE = "lilac_slicer";
    private final ResourceFiles resourceFiles;
    private final PersistedDataset persistedDataset;

    public LilacBamSlicer(final AlignmentPair alignmentPair, final ResourceFiles resourceFiles, final PersistedDataset persistedDataset) {
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
        return Stream.concat(buildCommands(getReferenceBamDownload().getLocalTargetPath(),
                                slicedBam(metadata.reference().sampleName())).stream(),
                        buildCommands(getTumorBamDownload().getLocalTargetPath(), slicedBam(metadata.tumor().sampleName())).stream())
                .collect(toList());

    }

    @Override
    public List<BashCommand> referenceOnlyCommands(final SomaticRunMetadata metadata) {
        return buildCommands(getReferenceBamDownload().getLocalTargetPath(), slicedBam(metadata.sampleName()));
    }

    @Override
    public List<BashCommand> tumorOnlyCommands(final SomaticRunMetadata metadata) {
        return buildCommands(getTumorBamDownload().getLocalTargetPath(), slicedBam(metadata.sampleName()));
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return ImmutableVirtualMachineJobDefinition.builder()
                .name(NAMESPACE.replaceAll("_", "-"))
                .startupCommand(bash)
                .performanceProfile(VirtualMachinePerformanceProfile.custom(8, 16))
                .namespacedResults(resultsDirectory)
                .build();
    }

    @Override
    public LilacBamSliceOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        Builder output = LilacBamSliceOutput.builder()
                .status(jobStatus)
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .addReportComponents(new EntireOutputComponent(bucket, Folder.root(), namespace(), resultsDirectory))
                .addAllDatatypes(addDatatypes(metadata));
        metadata.maybeTumor().ifPresent(tumor -> {
            String tumorBam = new File(slicedBam(metadata.tumor().sampleName())).getName();
            output.tumor(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(tumorBam)))
                    .tumorIndex(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(bai(tumorBam))));
        });
        metadata.maybeReference().ifPresent(reference -> {
            String referenceBam = new File(slicedBam(metadata.reference().sampleName())).getName();
            output.reference(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(referenceBam)))
                    .referenceIndex(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(bai(referenceBam))));
        });
        return output.build();
    }

    @Override
    public LilacBamSliceOutput skippedOutput(final SomaticRunMetadata metadata) {
        return LilacBamSliceOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public LilacBamSliceOutput persistedOutput(final SomaticRunMetadata metadata) {
        Builder outputBuilder = LilacBamSliceOutput.builder().status(PipelineStatus.PERSISTED).addAllDatatypes(addDatatypes(metadata));
        metadata.maybeReference().ifPresent(reference -> {
            String refName = reference.sampleName();
            outputBuilder.reference(persistedDataset.path(refName, DataType.LILAC_HLA_BAM)
                    .orElse(GoogleStorageLocation.of(metadata.bucket(),
                            PersistedLocations.blobForSet(metadata.set(), namespace(), basename(slicedBam(refName))))));
            outputBuilder.referenceIndex(persistedDataset.path(refName, DataType.LILAC_HLA_BAM_INDEX)
                    .orElse(GoogleStorageLocation.of(metadata.bucket(),
                            PersistedLocations.blobForSet(metadata.set(), namespace(), basename(bai(slicedBam(refName)))))));
        });
        metadata.maybeTumor().ifPresent(tumor -> {
            String tumorName = tumor.sampleName();
            outputBuilder.tumor(persistedDataset.path(tumorName, DataType.LILAC_HLA_BAM)
                    .orElse(GoogleStorageLocation.of(metadata.bucket(),
                            PersistedLocations.blobForSet(metadata.set(), namespace(), basename(slicedBam(tumorName))))));
            outputBuilder.tumorIndex(persistedDataset.path(tumorName, DataType.LILAC_HLA_BAM_INDEX)
                    .orElse(GoogleStorageLocation.of(metadata.bucket(),
                            PersistedLocations.blobForSet(metadata.set(), namespace(), basename(bai(slicedBam(tumorName)))))));
        });
        return outputBuilder.build();
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runTertiary() && !arguments.shallow();
    }

    private List<BashCommand> buildCommands(final String inputAlignmentFile, final String slicedBamFile) {
        return List.of(new SamtoolsCommand("view",
                "-L",
                resourceFiles.hlaRegionBed(),
                "-@",
                Bash.allCpus(),
                "-u",
                "-M",
                "-T",
                resourceFiles.refGenomeFile(),
                "-o",
                slicedBamFile,
                inputAlignmentFile), new SambambaCommand("index", slicedBamFile));
    }

    private String slicedBam(final String sampleName) {
        return VmDirectories.outputFile(sampleName + ".hla.bam");
    }

    private String bai(final String bam) {
        return bam + ".bai";
    }

    private String basename(final String fullname) {
        return new File(fullname).getName();
    }

    @Override
    public List<AddDatatype> addDatatypes(final SomaticRunMetadata metadata) {
        ArrayList<AddDatatype> datatypes = new ArrayList<>();
        metadata.maybeReference().ifPresent(r -> {
            String referenceBam = new File(slicedBam(metadata.reference().sampleName())).getName();
            datatypes.add(new AddDatatype(DataType.LILAC_HLA_BAM,
                    metadata.reference().barcode(),
                    new ArchivePath(Folder.root(), namespace(), referenceBam)));
            datatypes.add(new AddDatatype(DataType.LILAC_HLA_BAM_INDEX,
                    metadata.reference().barcode(),
                    new ArchivePath(Folder.root(), namespace(), bai(referenceBam))));
        });
        metadata.maybeTumor().ifPresent(t -> {
            String tumorBam = new File(slicedBam(metadata.tumor().sampleName())).getName();
            datatypes.add(new AddDatatype(DataType.LILAC_HLA_BAM,
                    metadata.tumor().barcode(),
                    new ArchivePath(Folder.root(), namespace(), tumorBam)));
            datatypes.add(new AddDatatype(DataType.LILAC_HLA_BAM_INDEX,
                    metadata.tumor().barcode(),
                    new ArchivePath(Folder.root(), namespace(), bai(tumorBam))));
        });
        return datatypes;
    }
}
