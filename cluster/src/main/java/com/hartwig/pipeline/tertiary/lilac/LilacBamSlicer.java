package com.hartwig.pipeline.tertiary.lilac;

import static java.util.stream.Collectors.toList;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.ImmutableVirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.SambambaCommand;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VirtualMachinePerformanceProfile;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.metadata.AddDatatype;
import com.hartwig.pipeline.metadata.ArchivePath;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.report.EntireOutputComponent;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.report.RunLogComponent;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.Namespace;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tertiary.TertiaryStage;
import com.hartwig.pipeline.tertiary.lilac.ImmutableLilacBamSliceOutput.Builder;

@Namespace(LilacBamSlicer.NAMESPACE)
public class LilacBamSlicer extends TertiaryStage<LilacBamSliceOutput> {
    public static final String NAMESPACE = "lilac_slicer";
    private final ResourceFiles resourceFiles;

    public LilacBamSlicer(final AlignmentPair alignmentPair, final ResourceFiles resourceFiles) {
        super(alignmentPair);
        this.resourceFiles = resourceFiles;
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
        return ImmutableVirtualMachineJobDefinition.builder().name(NAMESPACE.replaceAll("_", "-"))
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
                .addDatatypes(datatypes(metadata));
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
        return LilacBamSliceOutput.builder().status(PipelineStatus.PERSISTED).addDatatypes(datatypes(metadata)).build();
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runTertiary() && !arguments.shallow();
    }

    private List<BashCommand> buildCommands(final String inputBamFile, final String slicedBamFile) {
        return List.of(new SambambaCommand("slice", "-L", resourceFiles.hlaRegionBed(), "-o", slicedBamFile, inputBamFile),
                new SambambaCommand("index", slicedBamFile));
    }

    private String slicedBam(final String sampleName) {
        return VmDirectories.outputFile(sampleName + ".hla.bam");
    }

    private String bai(final String bam) {
        return bam + ".bai";
    }

    private AddDatatype[] datatypes(final SomaticRunMetadata metadata) {
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
        return datatypes.toArray(new AddDatatype[] {});
    }
}
