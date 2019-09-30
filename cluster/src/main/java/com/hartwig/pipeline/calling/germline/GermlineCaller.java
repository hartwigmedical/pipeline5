package com.hartwig.pipeline.calling.germline;

import static com.hartwig.pipeline.resource.ResourceNames.COSMIC;
import static com.hartwig.pipeline.resource.ResourceNames.DBNSFP;
import static com.hartwig.pipeline.resource.ResourceNames.DBSNPS;
import static com.hartwig.pipeline.resource.ResourceNames.GONL;
import static com.hartwig.pipeline.resource.ResourceNames.REFERENCE_GENOME;
import static com.hartwig.pipeline.resource.ResourceNames.SNPEFF;

import java.util.List;
import java.util.Map;

import com.google.cloud.storage.Storage;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.calling.SubStageInputOutput;
import com.hartwig.pipeline.calling.germline.command.SnpSiftDbnsfpAnnotation;
import com.hartwig.pipeline.calling.germline.command.SnpSiftFrequenciesAnnotation;
import com.hartwig.pipeline.calling.substages.CosmicAnnotation;
import com.hartwig.pipeline.calling.substages.SnpEff;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.execution.vm.ResourceDownload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.execution.vm.unix.MvCommand;
import com.hartwig.pipeline.execution.vm.unix.UnzipToDirectoryCommand;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.report.RunLogComponent;
import com.hartwig.pipeline.report.StartupScriptComponent;
import com.hartwig.pipeline.report.ZippedVcfAndIndexComponent;
import com.hartwig.pipeline.resource.GATKDictAlias;
import com.hartwig.pipeline.resource.ReferenceGenomeAlias;
import com.hartwig.pipeline.resource.Resource;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;

public class GermlineCaller implements Stage<GermlineCallerOutput, SingleSampleRunMetadata> {

    public static final String NAMESPACE = "germline_caller";
    public static final String TOOL_HEAP = "20G";
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

    private final InputDownload bamDownload;
    private final InputDownload baiDownload;
    private final OutputFile outputFile;

    public GermlineCaller(final AlignmentOutput alignmentOutput) {
        this.bamDownload = new InputDownload(alignmentOutput.finalBamLocation());
        this.baiDownload = new InputDownload(alignmentOutput.finalBaiLocation());
        outputFile = OutputFile.of(alignmentOutput.sample(), "germline", OutputFile.GZIPPED_VCF, false);
    }

    @Override
    public List<BashCommand> inputs() {
        return ImmutableList.of(bamDownload, baiDownload);
    }

    @Override
    public List<ResourceDownload> resources(final Storage storage, final String resourceBucket, final RuntimeBucket bucket) {
        return ImmutableList.of(ResourceDownload.from(bucket,
                new Resource(storage, resourceBucket, REFERENCE_GENOME, new ReferenceGenomeAlias().andThen(new GATKDictAlias()))),
                ResourceDownload.from(storage, resourceBucket, DBSNPS, bucket),
                ResourceDownload.from(storage, resourceBucket, SNPEFF, bucket),
                ResourceDownload.from(storage, resourceBucket, DBNSFP, bucket),
                ResourceDownload.from(storage, resourceBucket, COSMIC, bucket),
                ResourceDownload.from(storage, resourceBucket, GONL, bucket));
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> commands(final SingleSampleRunMetadata metadata, final Map<String, ResourceDownload> resources) {
        String snpEffConfig = resources.get(SNPEFF).find("config");
        String snpEffDb = resources.get(SNPEFF).find("zip");

        String referenceFasta = resources.get(REFERENCE_GENOME).find("fasta");
        String dbsnpVcf = resources.get(DBSNPS).find("vcf");

        SubStageInputOutput callerOutput =
                new GatkGermlineCaller(bamDownload.getLocalTargetPath(), referenceFasta, dbsnpVcf).andThen(new GenotypeGVCFs(referenceFasta,
                        dbsnpVcf)).apply(SubStageInputOutput.empty(metadata.sampleName()));

        SubStageInputOutput snpFilterOutput =
                new SelectVariants("snp", Lists.newArrayList("SNP", "NO_VARIATION"), referenceFasta).andThen(new VariantFiltration("snp",
                        SNP_FILTER_EXPRESSION,
                        referenceFasta)).apply(callerOutput);

        SubStageInputOutput indelFilterOutput =
                new SelectVariants("indels", Lists.newArrayList("INDEL", "MIXED"), referenceFasta).andThen(new VariantFiltration("indels",
                        INDEL_FILTER_EXPRESSION,
                        referenceFasta)).apply(snpFilterOutput);

        SubStageInputOutput finalOutput =
                new CombineFilteredVariants(indelFilterOutput.outputFile().path(), referenceFasta).andThen(new SnpEff(snpEffConfig))
                        .andThen(new SnpSiftDbnsfpAnnotation(resources.get(DBNSFP).find("txt.gz"), snpEffConfig))
                        .andThen(new CosmicAnnotation(resources.get(COSMIC).find("collapsed.vcf.gz"), "ID"))
                        .andThen(new SnpSiftFrequenciesAnnotation(resources.get(GONL).find("vcf.gz"), snpEffConfig))
                        .apply(indelFilterOutput);

        return ImmutableList.<BashCommand>builder().add(new UnzipToDirectoryCommand(VmDirectories.RESOURCES, snpEffDb))
                .addAll(finalOutput.bash())
                .add(new MvCommand(finalOutput.outputFile().path(), outputFile.path()))
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
                .maybeGermlineVcfLocation(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(outputFile.fileName())))
                .addReportComponents(new RunLogComponent(bucket, NAMESPACE, Folder.from(metadata), resultsDirectory))
                .addReportComponents(new StartupScriptComponent(bucket, NAMESPACE, Folder.from(metadata)))
                .addReportComponents(new ZippedVcfAndIndexComponent(bucket,
                        NAMESPACE,
                        Folder.from(metadata),
                        outputFile.fileName(),
                        outputFile.fileName(),
                        resultsDirectory))
                .build();
    }

    @Override
    public GermlineCallerOutput skippedOutput(final SingleSampleRunMetadata metadata) {
        return GermlineCallerOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runGermlineCaller() && !arguments.shallow();
    }
}