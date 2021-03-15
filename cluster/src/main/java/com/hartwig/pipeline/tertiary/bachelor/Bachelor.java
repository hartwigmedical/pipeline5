package com.hartwig.pipeline.tertiary.bachelor;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.calling.germline.GermlineCallerOutput;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.metadata.AddDatatype;
import com.hartwig.pipeline.metadata.ArchivePath;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.report.EntireOutputComponent;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.report.RunLogComponent;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tertiary.purple.PurpleOutput;

public class Bachelor implements Stage<BachelorOutput, SomaticRunMetadata> {

    public static final String NAMESPACE = "bachelor";
    public static final String VARIANT_TSV = ".bachelor.germline_variant.tsv";
    public static final String REPORTABLE_VARIANT_TSV = ".reportable_germline_variant.tsv";

    private final ResourceFiles resourceFiles;
    private final InputDownload purpleOutputDownload;
    private final InputDownload tumorBamDownload;
    private final InputDownload tumorBaiDownload;
    private final InputDownload germlineVcfDownload;
    private final InputDownload germlineVcfIndexDownload;

    public Bachelor(final ResourceFiles resourceFiles, final PurpleOutput purpleOutput, AlignmentOutput tumorAlignmentOutput,
            GermlineCallerOutput germlineCallerOutput) {
        this.resourceFiles = resourceFiles;
        this.purpleOutputDownload = new InputDownload(purpleOutput.outputLocations().outputDirectory());
        this.tumorBamDownload = new InputDownload(tumorAlignmentOutput.finalBamLocation());
        this.tumorBaiDownload = new InputDownload(tumorAlignmentOutput.finalBaiLocation());
        this.germlineVcfDownload = new InputDownload(germlineCallerOutput.germlineVcfLocation());
        this.germlineVcfIndexDownload = new InputDownload(germlineCallerOutput.germlineVcfIndexLocation());
    }

    @Override
    public List<BashCommand> inputs() {
        return ImmutableList.of(purpleOutputDownload, tumorBamDownload, tumorBaiDownload, germlineVcfDownload, germlineVcfIndexDownload);
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> commands(final SomaticRunMetadata metadata) {
        return Collections.singletonList(new BachelorCommand(metadata.tumor().sampleName(),
                germlineVcfDownload.getLocalTargetPath(),
                tumorBamDownload.getLocalTargetPath(),
                purpleOutputDownload.getLocalTargetPath(),
                resourceFiles.bachelorConfig(),
                resourceFiles.bachelorClinvarFilters(),
                resourceFiles.refGenomeFile(),
                VmDirectories.OUTPUT));
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.bachelor(bash, resultsDirectory);
    }

    @Override
    public BachelorOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        String bachelorVariantsTsv = metadata.tumor().sampleName() + VARIANT_TSV;
        String reportableVariantsTsv = metadata.tumor().sampleName() + REPORTABLE_VARIANT_TSV;
        return BachelorOutput.builder()
                .status(jobStatus)
                .maybeReportableVariants(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(reportableVariantsTsv)))
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .addReportComponents(new EntireOutputComponent(bucket, Folder.root(), NAMESPACE, resultsDirectory))
                .addDatatypes(new AddDatatype(DataType.BACHELOR,
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), bachelorVariantsTsv)))
                .addDatatypes(new AddDatatype(DataType.BACHELOR_REPORTABLE_VARIANTS,
                        metadata.barcode(),
                        new ArchivePath(Folder.root(), namespace(), reportableVariantsTsv)))
                .build();
    }

    @Override
    public BachelorOutput skippedOutput(final SomaticRunMetadata metadata) {
        return BachelorOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runTertiary() && arguments.runGermlineCaller() && !arguments.shallow();
    }
}
