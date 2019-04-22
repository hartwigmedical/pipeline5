package com.hartwig.pipeline.calling.somatic;

import static com.google.common.collect.Lists.*;
import static com.hartwig.pipeline.execution.vm.OutputUpload.OUTPUT_DIRECTORY;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.OutputUpload;
import com.hartwig.pipeline.execution.vm.ResourceDownload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.io.GoogleStorageLocation;
import com.hartwig.pipeline.io.ResultsDirectory;
import com.hartwig.pipeline.io.RuntimeBucket;
import com.hartwig.pipeline.resource.GATKDictAlias;
import com.hartwig.pipeline.resource.ReferenceGenomeAlias;
import com.hartwig.pipeline.resource.Resource;
import com.hartwig.pipeline.resource.ResourceLocation;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SomaticCaller {

    private static final String STRELKA_ANALYSIS_DIRECTORY = "/strelkaAnalysis";
    private static final int STRELKA_THREADS = 8;

    private final Arguments arguments;
    private final ComputeEngine computeEngine;
    private final Storage storage;
    private final ResultsDirectory resultsDirectory;

    SomaticCaller(final Arguments arguments, final ComputeEngine computeEngine, final Storage storage,
            final ResultsDirectory resultsDirectory) {
        this.arguments = arguments;
        this.computeEngine = computeEngine;
        this.storage = storage;
        this.resultsDirectory = resultsDirectory;
    }

    public SomaticCallerOutput run(AlignmentPair pair) {
        RuntimeBucket runtimeBucket =
                RuntimeBucket.from(storage, pair.reference().sample().name(), pair.tumor().sample().name(), arguments);

        ResourceDownload referenceGenomeDownload = new ResourceDownload(new Resource(storage,
                arguments.referenceGenomeBucket(),
                "reference_genome",
                new ReferenceGenomeAlias().andThen(new GATKDictAlias())).copyInto(runtimeBucket), runtimeBucket);
        String referenceGenomeFile = referenceGenomeDownload.find("fa", "fasta");

        String combinedVcfOutput = OUTPUT_DIRECTORY + "/combined.vcf";

        BashStartupScript strelkaBash = BashStartupScript.of(runtimeBucket.name());

        runStrelkaAndCombineVcfs(new InputDownload(pair.reference().finalBamLocation()),
                new InputDownload(pair.reference().finalBaiLocation()),
                new InputDownload(pair.tumor().finalBamLocation()),
                new InputDownload(pair.tumor().finalBaiLocation()),
                referenceGenomeDownload,
                referenceGenomeFile,
                new ResourceDownload(new Resource(storage, "strelka_config", "strelka_config").copyInto(runtimeBucket), runtimeBucket),
                combinedVcfOutput,
                strelkaBash);
        applyAnnotations(storage, runtimeBucket, combinedVcfOutput, strelkaBash);
        strelkaBash.addCommand(new OutputUpload(GoogleStorageLocation.of(runtimeBucket.name(), resultsDirectory.path())));
        computeEngine.submit(runtimeBucket, VirtualMachineJobDefinition.somaticCalling(strelkaBash));

        return SomaticCallerOutput.builder().build();
    }

    private static void runStrelkaAndCombineVcfs(final InputDownload downloadReferenceBam, final InputDownload downloadReferenceBai,
            final InputDownload downloadTumorBam, final InputDownload downloadTumorBai, final ResourceDownload referenceGenomeDownload,
            final String referenceGenomeFile, final ResourceDownload configDownload, final String combinedVcfOutput,
            final BashStartupScript strelkaBash) {
        String strelkaConfigFile = configDownload.find("ini");
        String strelkaAnalysisOutput = OUTPUT_DIRECTORY + STRELKA_ANALYSIS_DIRECTORY;
        strelkaBash.addCommand(downloadReferenceBam)
                .addCommand(downloadReferenceBai)
                .addCommand(downloadTumorBam)
                .addCommand(downloadTumorBai)
                .addCommand(referenceGenomeDownload)
                .addCommand(configDownload)
                .addCommand(new ConfigureStrelkaWorkflowCommand(downloadTumorBam.getLocalTargetPath(),
                        downloadReferenceBam.getLocalTargetPath(),
                        strelkaConfigFile,
                        referenceGenomeFile,
                        strelkaAnalysisOutput))
                .addCommand(new MakeStrelka(strelkaAnalysisOutput, STRELKA_THREADS))
                .addCommand(new CombineVcfsCommand(referenceGenomeFile,
                        strelkaAnalysisOutput + "/results/passed.somatic.snvs.vcf",
                        strelkaAnalysisOutput + "/results/passed.somatic.snvs.vcf",
                        combinedVcfOutput));
    }

    private static void applyAnnotations(final Storage storage, final RuntimeBucket runtimeBucket, final String combinedVcfOutput,
            final BashStartupScript strelkaBash) {
        String mappabilityAnnotatedVcf = OUTPUT_DIRECTORY + "/mappability.annotated.vcf.gz";
        final Resource annotationResource = new Resource(storage, "hg19_mappability_tracks", "hg19_mappability_tracks");
        annotate(SomaticCaller::mappabilityAnnotation,
                combinedVcfOutput,
                mappabilityAnnotatedVcf,
                strelkaBash,
                new ResourceDownload(annotationResource.copyInto(runtimeBucket), runtimeBucket));
        String germlinePonAnnotatedVcf = OUTPUT_DIRECTORY + "/germline.pon.annotated.vcf.gz";
        final ResourceDownload ponDownload =
                new ResourceDownload(new Resource(storage, "pon-v2", "pon-v2").copyInto(runtimeBucket), runtimeBucket);
        annotate(SomaticCaller::germlinePonAnnotation, mappabilityAnnotatedVcf, germlinePonAnnotatedVcf, strelkaBash, ponDownload);
        annotate(SomaticCaller::somaticPonAnnotation,
                germlinePonAnnotatedVcf,
                OUTPUT_DIRECTORY + "/somatic.pon.annotated.vcf.gz",
                strelkaBash,
                ponDownload);
    }

    @NotNull
    private static ArrayList<String> mappabilityAnnotation(final ResourceDownload resourceDownload) {
        return newArrayList(resourceDownload.find("bed.gz"), "-h", resourceDownload.find("hdr"), "-c", "CHROM,FROM,TO,-,MAPPABILITY");
    }

    @NotNull
    private static ArrayList<String> germlinePonAnnotation(final ResourceDownload resourceDownload) {
        return newArrayList(resourceDownload.find("GERMLINE_PON.vcf.gz"), "-c", "GERMLINE_PON_COUNT");
    }

    @NotNull
    private static ArrayList<String> somaticPonAnnotation(final ResourceDownload resourceDownload) {
        return newArrayList(resourceDownload.find("SOMATIC_PON.vcf.gz"), "-c", "SOMATIC_PON_COUNT");
    }

    private static void annotate(Function<ResourceDownload, List<String>> arguments, String inputVcf, String outputVcf,
            BashStartupScript startupScript, final ResourceDownload resourceDownload) {
        startupScript.addCommand(resourceDownload)
                .addCommand(new BcfToolsAnnotationCommand(arguments.apply(resourceDownload), inputVcf, outputVcf))
                .addCommand(new TabixCommand(outputVcf));
    }
}
