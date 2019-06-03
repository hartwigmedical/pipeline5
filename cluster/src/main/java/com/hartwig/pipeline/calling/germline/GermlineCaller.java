package com.hartwig.pipeline.calling.germline;

import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.calling.SubStageInputOutput;
import com.hartwig.pipeline.calling.germline.command.SnpSiftDbnsfpAnnotation;
import com.hartwig.pipeline.calling.germline.command.SnpSiftFrequenciesAnnotation;
import com.hartwig.pipeline.calling.substages.CosmicAnnotation;
import com.hartwig.pipeline.calling.substages.SnpEff;
import com.hartwig.pipeline.execution.JobStatus;
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
import com.hartwig.pipeline.report.RunLogComponent;
import com.hartwig.pipeline.report.SingleFileComponent;
import com.hartwig.pipeline.resource.GATKDictAlias;
import com.hartwig.pipeline.resource.ReferenceGenomeAlias;
import com.hartwig.pipeline.resource.Resource;
import com.hartwig.pipeline.resource.ResourceNames;

public class GermlineCaller {

    public static final String NAMESPACE = "germline_caller";

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

    public GermlineCallerOutput run(AlignmentOutput alignmentOutput) {

        if (!arguments.runGermlineCaller()) {
            return GermlineCallerOutput.builder().status(JobStatus.SKIPPED).build();
        }

        String sampleName = alignmentOutput.sample().name();
        RuntimeBucket bucket = RuntimeBucket.from(storage, NAMESPACE, sampleName, arguments);

        ResourceDownload referenceGenome = ResourceDownload.from(bucket,
                new Resource(storage,
                        arguments.resourceBucket(),
                        ResourceNames.REFERENCE_GENOME,
                        new ReferenceGenomeAlias().andThen(new GATKDictAlias())));
        ResourceDownload knownSnps = ResourceDownload.from(storage, arguments.resourceBucket(), ResourceNames.KNOWN_SNPS, bucket);
        ResourceDownload snpEffResource = ResourceDownload.from(storage, arguments.resourceBucket(), ResourceNames.SNPEFF, bucket);
        ResourceDownload dbNSFPResource = ResourceDownload.from(storage, arguments.resourceBucket(), ResourceNames.DBNSFP, bucket);
        ResourceDownload cosmicResourceDownload = ResourceDownload.from(storage, arguments.resourceBucket(), ResourceNames.COSMIC, bucket);
        ResourceDownload frequencyDbDownload = ResourceDownload.from(storage, arguments.resourceBucket(), ResourceNames.GONL, bucket);

        InputDownload bamDownload = new InputDownload(alignmentOutput.finalBamLocation());
        BashStartupScript startupScript = BashStartupScript.of(bucket.name())
                .addCommand(bamDownload)
                .addCommand(new InputDownload(alignmentOutput.finalBaiLocation()))
                .addCommand(referenceGenome)
                .addCommand(knownSnps)
                .addCommand(snpEffResource)
                .addCommand(cosmicResourceDownload)
                .addCommand(dbNSFPResource)
                .addCommand(frequencyDbDownload);

        String snpEffConfig = snpEffResource.find("config");
        SubStageInputOutput finalOutput = new GatkGermlineCaller(bamDownload.getLocalTargetPath(),
                referenceGenome.find("fasta"),
                knownSnps.find("vcf")).andThen(new SnpEff(snpEffConfig))
                .andThen(new SnpSiftDbnsfpAnnotation(dbNSFPResource.find("txt.gz"), snpEffConfig))
                .andThen(new CosmicAnnotation(cosmicResourceDownload.find("vcf.gz")))
                .andThen(new SnpSiftFrequenciesAnnotation(frequencyDbDownload.find("vcf.gz"), snpEffConfig))
                .apply(SubStageInputOutput.of(alignmentOutput.sample().name(), OutputFile.empty(), startupScript));

        startupScript.addCommand(new OutputUpload(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path())));

        ImmutableGermlineCallerOutput.Builder outputBuilder = GermlineCallerOutput.builder();
        JobStatus status = executor.submit(bucket, VirtualMachineJobDefinition.germlineCalling(startupScript, resultsDirectory));
        return outputBuilder.status(status)
                .addReportComponents(new RunLogComponent(bucket, NAMESPACE, sampleName, resultsDirectory))
                .addReportComponents(new SingleFileComponent(bucket,
                        NAMESPACE,
                        sampleName,
                        finalOutput.outputFile().fileName(),
                        resultsDirectory))
                .build();
    }
}