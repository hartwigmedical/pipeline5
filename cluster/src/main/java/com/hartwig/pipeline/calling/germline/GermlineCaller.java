package com.hartwig.pipeline.calling.germline;

import java.util.Map;

import com.google.cloud.storage.Storage;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.calling.FinalSubStage;
import com.hartwig.pipeline.calling.SubStageInputOutput;
import com.hartwig.pipeline.calling.germline.command.SnpSiftDbnsfpAnnotation;
import com.hartwig.pipeline.calling.germline.command.SnpSiftFrequenciesAnnotation;
import com.hartwig.pipeline.calling.substages.CosmicAnnotation;
import com.hartwig.pipeline.calling.substages.SnpEff;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.execution.vm.OutputUpload;
import com.hartwig.pipeline.execution.vm.ResourceDownload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.io.GoogleStorageLocation;
import com.hartwig.pipeline.io.ResultsDirectory;
import com.hartwig.pipeline.io.RuntimeBucket;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.report.RunLogComponent;
import com.hartwig.pipeline.report.ZippedVcfAndIndexComponent;
import com.hartwig.pipeline.resource.GATKDictAlias;
import com.hartwig.pipeline.resource.ReferenceGenomeAlias;
import com.hartwig.pipeline.resource.Resource;
import com.hartwig.pipeline.resource.ResourceNames;
import com.hartwig.pipeline.trace.StageTrace;

public class GermlineCaller {

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

    private final Arguments arguments;
    private final ComputeEngine executor;
    private final Storage storage;
    private final ResultsDirectory resultsDirectory;

    GermlineCaller(final Arguments arguments, final ComputeEngine executor, final Storage storage,
            final ResultsDirectory resultsDirectory) {
        this.arguments = arguments;
        this.executor = executor;
        this.storage = storage;
        this.resultsDirectory = resultsDirectory;
    }

    public GermlineCallerOutput run(SingleSampleRunMetadata metadata, AlignmentOutput alignmentOutput) {

        if (!arguments.runGermlineCaller()) {
            return GermlineCallerOutput.builder().status(PipelineStatus.SKIPPED).build();
        }

        StageTrace trace = new StageTrace(NAMESPACE, StageTrace.ExecutorType.COMPUTE_ENGINE).start();

        String sampleName = alignmentOutput.sample();
        RuntimeBucket bucket = RuntimeBucket.from(storage, NAMESPACE, metadata, arguments);

        ResourceDownload referenceGenome = ResourceDownload.from(bucket,
                new Resource(storage,
                        arguments.resourceBucket(),
                        ResourceNames.REFERENCE_GENOME,
                        new ReferenceGenomeAlias().andThen(new GATKDictAlias())));
        ResourceDownload dbSnps = ResourceDownload.from(storage, arguments.resourceBucket(), ResourceNames.DBSNPS, bucket);
        ResourceDownload snpEffResource = ResourceDownload.from(storage, arguments.resourceBucket(), ResourceNames.SNPEFF, bucket);
        ResourceDownload dbNSFPResource = ResourceDownload.from(storage, arguments.resourceBucket(), ResourceNames.DBNSFP, bucket);
        ResourceDownload cosmicResourceDownload = ResourceDownload.from(storage, arguments.resourceBucket(), ResourceNames.COSMIC, bucket);
        ResourceDownload frequencyDbDownload = ResourceDownload.from(storage, arguments.resourceBucket(), ResourceNames.GONL, bucket);

        InputDownload bamDownload = new InputDownload(alignmentOutput.finalBamLocation());
        BashStartupScript startupScript = BashStartupScript.of(bucket.name())
                .addCommand(bamDownload)
                .addCommand(new InputDownload(alignmentOutput.finalBaiLocation()))
                .addCommand(referenceGenome)
                .addCommand(dbSnps)
                .addCommand(snpEffResource)
                .addCommand(cosmicResourceDownload)
                .addCommand(dbNSFPResource)
                .addCommand(frequencyDbDownload);

        String snpEffConfig = snpEffResource.find("config");

        String referenceFasta = referenceGenome.find("fasta");
        String dbsnpVcf = dbSnps.find("vcf");
        SubStageInputOutput callerOutput =
                new GatkGermlineCaller(bamDownload.getLocalTargetPath(), referenceFasta, dbsnpVcf).andThen(new GenotypeGVCFs(referenceFasta,
                        dbsnpVcf)).apply(SubStageInputOutput.of(alignmentOutput.sample(), OutputFile.empty(), startupScript));

        SubStageInputOutput snpFilterOutput =
                new SelectVariants("snp", Lists.newArrayList("SNP", "NO_VARIATION"), referenceFasta).andThen(new VariantFiltration("snp",
                        SNP_FILTER_EXPRESSION,
                        referenceFasta)).apply(callerOutput);

        SubStageInputOutput indelFilterOutput =
                new SelectVariants("indels", Lists.newArrayList("INDEL", "MIXED"), referenceFasta).andThen(new VariantFiltration("indels",
                        INDEL_FILTER_EXPRESSION,
                        referenceFasta)).apply(callerOutput);

        SubStageInputOutput finalOutput =
                new CombineFilteredVariants(indelFilterOutput.outputFile().path(), referenceFasta).andThen(new SnpEff(snpEffConfig))
                        .andThen(new SnpSiftDbnsfpAnnotation(dbNSFPResource.find("txt.gz"), snpEffConfig))
                        .andThen(new CosmicAnnotation(cosmicResourceDownload.find("collapsed.vcf.gz"), "ID"))
                        .andThen(FinalSubStage.of(new SnpSiftFrequenciesAnnotation(frequencyDbDownload.find("vcf.gz"), snpEffConfig)))
                        .apply(snpFilterOutput);

        startupScript.addCommand(new OutputUpload(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path())));

        ImmutableGermlineCallerOutput.Builder outputBuilder = GermlineCallerOutput.builder();
        PipelineStatus status = executor.submit(bucket, VirtualMachineJobDefinition.germlineCalling(startupScript, resultsDirectory));
        trace.stop();
        return outputBuilder.status(status)
                .addReportComponents(new RunLogComponent(bucket, NAMESPACE, Folder.from(metadata), resultsDirectory))
                .addReportComponents(new ZippedVcfAndIndexComponent(bucket,
                        NAMESPACE,
                        Folder.from(metadata),
                        finalOutput.outputFile().fileName(),
                        OutputFile.of(alignmentOutput.sample(), "germline", OutputFile.GZIPPED_VCF, false).fileName(),
                        resultsDirectory))
                .build();
    }
}