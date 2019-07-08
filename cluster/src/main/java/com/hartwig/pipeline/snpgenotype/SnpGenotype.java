package com.hartwig.pipeline.snpgenotype;

import static java.lang.String.format;

import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.OutputUpload;
import com.hartwig.pipeline.execution.vm.ResourceDownload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.report.RunLogComponent;
import com.hartwig.pipeline.report.SingleFileComponent;
import com.hartwig.pipeline.resource.GATKDictAlias;
import com.hartwig.pipeline.resource.ReferenceGenomeAlias;
import com.hartwig.pipeline.resource.Resource;
import com.hartwig.pipeline.resource.ResourceNames;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.trace.StageTrace;

public class SnpGenotype {

    public static final String NAMESPACE = "snp_genotype";

    private static final String OUTPUT_FILENAME = "snp_genotype_output.vcf";

    private final Arguments arguments;
    private final ComputeEngine executor;
    private final Storage storage;
    private final ResultsDirectory resultsDirectory;

    public SnpGenotype(final Arguments arguments, final ComputeEngine executor, final Storage storage,
            final ResultsDirectory resultsDirectory) {
        this.arguments = arguments;
        this.executor = executor;
        this.storage = storage;
        this.resultsDirectory = resultsDirectory;
    }

    public SnpGenotypeOutput run(SingleSampleRunMetadata metadata, AlignmentOutput alignmentOutput) {

        if (!arguments.runSnpGenotyper()) {
            return SnpGenotypeOutput.builder().status(PipelineStatus.SKIPPED).build();
        }

        StageTrace trace = new StageTrace(NAMESPACE, StageTrace.ExecutorType.COMPUTE_ENGINE).start();

        String sampleName = alignmentOutput.sample();
        RuntimeBucket bucket = RuntimeBucket.from(storage, NAMESPACE, metadata, arguments);

        ResourceDownload referenceGenomeDownload = ResourceDownload.from(bucket,
                new Resource(storage,
                        arguments.resourceBucket(),
                        ResourceNames.REFERENCE_GENOME,
                        new ReferenceGenomeAlias().andThen(new GATKDictAlias())));
        ResourceDownload genotypeSnps = ResourceDownload.from(storage, arguments.resourceBucket(), ResourceNames.GENOTYPE_SNPS, bucket);

        InputDownload bamDownload = new InputDownload(alignmentOutput.finalBamLocation());
        BashStartupScript startupScript = BashStartupScript.of(bucket.name())
                .addCommand(bamDownload)
                .addCommand(new InputDownload(alignmentOutput.finalBaiLocation()))
                .addCommand(referenceGenomeDownload)
                .addCommand(genotypeSnps)
                .addCommand(new SnpGenotypeCommand(bamDownload.getLocalTargetPath(),
                        referenceGenomeDownload.find("fasta"),
                        genotypeSnps.find("26SNPtaq.vcf"),
                        format("%s/%s", VmDirectories.OUTPUT, OUTPUT_FILENAME)))
                .addCommand(new OutputUpload(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path())));

        PipelineStatus status = executor.submit(bucket, VirtualMachineJobDefinition.snpGenotyping(startupScript, resultsDirectory));
        trace.stop();
        return SnpGenotypeOutput.builder()
                .status(status)
                .addReportComponents(new RunLogComponent(bucket, NAMESPACE, Folder.from(metadata), resultsDirectory))
                .addReportComponents(new SingleFileComponent(bucket,
                        NAMESPACE,
                        Folder.from(metadata),
                        OUTPUT_FILENAME,
                        OUTPUT_FILENAME,
                        resultsDirectory))
                .build();
    }
}
