package com.hartwig.pipeline.calling.germline;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.calling.substages.SnpEff;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.execution.vm.unix.MvCommand;
import com.hartwig.pipeline.execution.vm.unix.UnzipToDirectoryCommand;
import com.hartwig.pipeline.metadata.AddDatatypeToFile;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.report.RunLogComponent;
import com.hartwig.pipeline.report.StartupScriptComponent;
import com.hartwig.pipeline.report.ZippedVcfAndIndexComponent;
import com.hartwig.pipeline.reruns.PersistedDataset;
import com.hartwig.pipeline.reruns.PersistedLocations;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.stages.SubStageInputOutput;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;

public class GermlineCaller implements Stage<GermlineCallerOutput, SingleSampleRunMetadata> {

    public static final String NAMESPACE = "germline_caller";
    public static final String TOOL_HEAP = "29G";
    private static final Map<String, String> SNP_FILTER_EXPRESSION =
            ImmutableMap.<String, String>builder().put("SNP_LowQualityDepth", "QD < 2.0")
                    .put("SNP_MappingQuality", "MQ < 40.0")
                    .put("SNP_StrandBias", "FS > 60.0")
                    .put("SNP_HaplotypeScoreHigh", "HaplotypeScore > 13.0")
                    .put("SNP_MQRankSumLow", "MQRankSum < -12.5")
                    .put("SNP_ReadPosRankSumLow", "ReadPosRankSum < -8.0")
                    .build();
    private static final Map<String, String> INDEL_FILTER_EXPRESSION = ImmutableMap.of("INDEL_LowQualityDepth",
            "QD < 2.0",
            "INDEL_StrandBias",
            "FS > 200.0",
            "INDEL_ReadPosRankSumLow",
            "ReadPosRankSum < -20.0");
    private static final String GERMLINE_VARIANTS_DATA_TYPE = "germline_variants";
    private static final String GERMLINE_VARIANTS_INDEX_DATATYPE = "germline_variants_index";

    private final ResourceFiles resourceFiles;
    private final InputDownload bamDownload;
    private final InputDownload baiDownload;
    private final OutputFile outputFile;
    private final PersistedDataset persistedDataset;

    public GermlineCaller(final AlignmentOutput alignmentOutput, final ResourceFiles resourceFiles,
            final PersistedDataset persistedDataset) {
        this.resourceFiles = resourceFiles;
        this.bamDownload = new InputDownload(alignmentOutput.finalBamLocation());
        this.baiDownload = new InputDownload(alignmentOutput.finalBaiLocation());
        outputFile = GermlineCallerOutput.outputFile(alignmentOutput.sample());
        this.persistedDataset = persistedDataset;
    }

    @Override
    public List<BashCommand> inputs() {
        return ImmutableList.of(bamDownload, baiDownload);
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> commands(final SingleSampleRunMetadata metadata) {

        String referenceFasta = resourceFiles.refGenomeFile();

        SubStageInputOutput callerOutput =
                new GatkGermlineCaller(bamDownload.getLocalTargetPath(), referenceFasta).andThen(new GenotypeGVCFs(referenceFasta))
                        .apply(SubStageInputOutput.empty(metadata.sampleName()));

        SubStageInputOutput snpFilterOutput =
                new SelectVariants("snp", Lists.newArrayList("SNP", "NO_VARIATION"), referenceFasta).andThen(new VariantFiltration("snp",
                        SNP_FILTER_EXPRESSION,
                        referenceFasta)).apply(callerOutput);

        SubStageInputOutput indelFilterOutput =
                new SelectVariants("indels", Lists.newArrayList("INDEL", "MIXED"), referenceFasta).andThen(new VariantFiltration("indels",
                        INDEL_FILTER_EXPRESSION,
                        referenceFasta))
                        .apply(SubStageInputOutput.of(metadata.sampleName(), callerOutput.outputFile(), Collections.emptyList()));

        SubStageInputOutput combinedFilters = snpFilterOutput.combine(indelFilterOutput);

        SubStageInputOutput finalOutput =
                new CombineFilteredVariants(indelFilterOutput.outputFile().path(), referenceFasta).andThen(new SnpEff(resourceFiles))
                        .apply(combinedFilters);

        return ImmutableList.<BashCommand>builder().add(new UnzipToDirectoryCommand(VmDirectories.RESOURCES, resourceFiles.snpEffDb()))
                .addAll(finalOutput.bash())
                .add(new MvCommand(finalOutput.outputFile().path(), outputFile.path()))
                .add(new MvCommand(finalOutput.outputFile().path() + ".tbi", outputFile.path() + ".tbi"))
                .build();
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.germlineCalling(bash, resultsDirectory);
    }

    @Override
    public GermlineCallerOutput output(final SingleSampleRunMetadata metadata, final PipelineStatus status, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        return GermlineCallerOutput.builder()
                .status(status)
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .maybeGermlineVcfLocation(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(outputFile.fileName())))
                .maybeGermlineVcfIndexLocation(GoogleStorageLocation.of(bucket.name(),
                        resultsDirectory.path(FileTypes.tabixIndex(outputFile.fileName()))))
                .addReportComponents(new RunLogComponent(bucket, NAMESPACE, Folder.from(metadata), resultsDirectory))
                .addReportComponents(new StartupScriptComponent(bucket, NAMESPACE, Folder.from(metadata)))
                .addReportComponents(new ZippedVcfAndIndexComponent(bucket,
                        NAMESPACE,
                        Folder.from(metadata),
                        outputFile.fileName(),
                        outputFile.fileName(),
                        resultsDirectory))
                .addFurtherOperations(new AddDatatypeToFile(DataType.GERMLINE_VARIANTS,
                        Folder.from(metadata),
                        namespace(),
                        outputFile.fileName(),
                        metadata.barcode()))
                .build();
    }

    @Override
    public GermlineCallerOutput skippedOutput(final SingleSampleRunMetadata metadata) {
        return GermlineCallerOutput.builder()
                .status(PipelineStatus.SKIPPED)
                .maybeGermlineVcfLocation(skipped())
                .maybeGermlineVcfIndexLocation(skipped())
                .build();
    }

    @Override
    public GermlineCallerOutput persistedOutput(final SingleSampleRunMetadata metadata) {
        String vcfPath = persistedDataset.file(metadata, DataType.GERMLINE_VARIANTS)
                .orElse(PersistedLocations.blobForSingle(metadata.set(),
                        metadata.sampleName(),
                        GermlineCaller.NAMESPACE,
                        GermlineCallerOutput.outputFile(metadata.sampleName()).fileName()));
        return GermlineCallerOutput.builder()
                .status(PipelineStatus.PERSISTED)
                .maybeGermlineVcfLocation(GoogleStorageLocation.of(metadata.bucket(), vcfPath))
                .maybeGermlineVcfIndexLocation(GoogleStorageLocation.of(metadata.bucket(), FileTypes.tabixIndex(vcfPath)))
                .build();
    }

    private static GoogleStorageLocation skipped() {
        return GoogleStorageLocation.of("", "");
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runGermlineCaller() && !arguments.shallow();
    }
}