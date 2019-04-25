package com.hartwig.pipeline.calling.somatic;

import static com.google.common.collect.Lists.newArrayList;
import static com.hartwig.pipeline.execution.vm.OutputUpload.OUTPUT_DIRECTORY;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import com.google.cloud.storage.Storage;
import com.google.common.collect.Lists;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.OutputUpload;
import com.hartwig.pipeline.execution.vm.ResourceDownload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.io.GoogleStorageLocation;
import com.hartwig.pipeline.io.ResultsDirectory;
import com.hartwig.pipeline.io.RuntimeBucket;
import com.hartwig.pipeline.resource.GATKDictAlias;
import com.hartwig.pipeline.resource.ReferenceGenomeAlias;
import com.hartwig.pipeline.resource.Resource;

import org.jetbrains.annotations.NotNull;

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
        String tumorName = pair.tumor().sample().name();
        String referenceName = pair.reference().sample().name();
        RuntimeBucket runtimeBucket = RuntimeBucket.from(storage, referenceName, tumorName, arguments);

        ResourceDownload referenceGenomeDownload = new ResourceDownload(new Resource(storage,
                arguments.referenceGenomeBucket(),
                "reference_genome",
                new ReferenceGenomeAlias().andThen(new GATKDictAlias())).copyInto(runtimeBucket), runtimeBucket);
        String referenceGenomePath = referenceGenomeDownload.find("fa", "fasta");
        BashStartupScript bash = BashStartupScript.of(runtimeBucket.name());

        InputDownload tumorBam = new InputDownload(pair.tumor().finalBamLocation());
        InputDownload referenceBam = new InputDownload(pair.reference().finalBamLocation());
        String combinedVcf = runStrelkaAndCombineVcfs(referenceBam,
                new InputDownload(pair.reference().finalBaiLocation()),
                tumorBam,
                new InputDownload(pair.tumor().finalBaiLocation()),
                referenceGenomeDownload,
                referenceGenomePath,
                new ResourceDownload(new Resource(storage, "strelka_config", "strelka_config").copyInto(runtimeBucket), runtimeBucket),
                bash);

        String annotatedVcf = applyAnnotations(storage, runtimeBucket, combinedVcf, bash);
        String postProcessedVcf = strelkaPostProcess(storage, pair, runtimeBucket, bash, tumorBam, annotatedVcf);
        String filteredVcf = filter(bash, postProcessedVcf);
        String sageVcf =
                sage(storage, tumorName, referenceName, runtimeBucket, referenceGenomePath, bash, tumorBam, referenceBam, filteredVcf);
        String snpEffVcf = snpEff(storage, runtimeBucket, bash, sageVcf);
        String dbsnpVcf = dbsnp(storage, runtimeBucket, bash, snpEffVcf);
        String cosmicVcf = cosmic(storage, runtimeBucket, bash, dbsnpVcf);

        bash.addCommand(new OutputUpload(GoogleStorageLocation.of(runtimeBucket.name(), resultsDirectory.path())));
        computeEngine.submit(runtimeBucket, VirtualMachineJobDefinition.somaticCalling(bash));

        return SomaticCallerOutput.builder()
                .finalSomaticVcf(GoogleStorageLocation.of(runtimeBucket.name(), resultsDirectory.path(cosmicVcf)))
                .build();
    }

    private static String cosmic(final Storage storage, final RuntimeBucket runtimeBucket, final BashStartupScript bash,
            final String dbsnpVcf) {
        Resource cosmicResource = new Resource(storage, "cosmic_v85", "cosmic_v85");
        ResourceDownload cosmicResourceDownload = new ResourceDownload(cosmicResource.copyInto(runtimeBucket), runtimeBucket);
        String cosmicVcf = OUTPUT_DIRECTORY + "/cosmic.vcf.gz";
        bash.addCommand(cosmicResourceDownload)
                .addCommand(new BcfToolsAnnotationCommand(Lists.newArrayList(cosmicResourceDownload.find("vcf.gz"), "-c", "ID, INFO"),
                        dbsnpVcf,
                        cosmicVcf));
        return cosmicVcf;
    }

    private static String dbsnp(final Storage storage, final RuntimeBucket runtimeBucket, final BashStartupScript bash,
            final String snpEffVcf) {
        Resource dbsnpResource = new Resource(storage, "known_snps", "known_snps");
        ResourceDownload dbsnpResourceDownload = new ResourceDownload(dbsnpResource.copyInto(runtimeBucket), runtimeBucket);
        String dbsnpAnnotatedVcf = OUTPUT_DIRECTORY + "/dbsnp.vcf.gz";
        bash.addCommand(dbsnpResourceDownload)
                .addCommand(new BcfToolsAnnotationCommand(Lists.newArrayList(dbsnpResourceDownload.find("vcf.gz"), "-c", "ID"),
                        snpEffVcf,
                        dbsnpAnnotatedVcf))
                .addCommand(new TabixCommand(dbsnpAnnotatedVcf));
        return dbsnpAnnotatedVcf;
    }

    private static String snpEff(final Storage storage, final RuntimeBucket runtimeBucket, final BashStartupScript bash,
            final String sageVcf) {
        Resource snpEffResource = new Resource(storage, "snpeff", "snpeff");
        ResourceDownload snpEffResourceDownload = new ResourceDownload(snpEffResource.copyInto(runtimeBucket), runtimeBucket);
        String snpEffVcf = OUTPUT_DIRECTORY + "/snpeff.annotated.vcf";
        String snpEffZippedVcf = snpEffVcf + ".gz";
        bash.addCommand(snpEffResourceDownload)
                .addCommand(new SnpEffCommand(snpEffResourceDownload.find("config"), sageVcf, snpEffVcf))
                .addCommand(new BgzipCommand(snpEffVcf))
                .addCommand(new TabixCommand(snpEffZippedVcf));
        return snpEffZippedVcf;
    }

    private static String sage(final Storage storage, final String tumorName, final String referenceName, final RuntimeBucket runtimeBucket,
            final String referenceGenomePath, final BashStartupScript bash, final InputDownload tumorBam, final InputDownload referenceBam,
            final String filteredVcf) {
        ResourceDownload sageResourceDownload =
                new ResourceDownload(new Resource(storage, "sage-pilot", "sage-pilot").copyInto(runtimeBucket), runtimeBucket);
        String sageUnfilteredVcf = VmDirectories.OUTPUT + "/sage.hotspots.unfiltered.vcf.gz";
        String sageFilteredVcf = OUTPUT_DIRECTORY + "/sage.hotspots.filtered.vcf.gz";
        String sagePonVcf = OUTPUT_DIRECTORY + "/sage.hotspots.pon.vcf.gz";
        String sagePonFilteredVcf = OUTPUT_DIRECTORY + "/sage.hotspots.pon.filtered.vcf.gz";
        String sageOutputVcf = OUTPUT_DIRECTORY + "/sage.vcf.gz";
        bash.addCommand(sageResourceDownload)
                .addCommand(new SageApplicationCommand(tumorName,
                        tumorBam.getLocalTargetPath(),
                        referenceName,
                        referenceBam.getLocalTargetPath(),
                        sageResourceDownload.find("tsv"),
                        sageResourceDownload.find("bed"),
                        referenceGenomePath,
                        sageUnfilteredVcf))
                .addCommand(new TabixCommand(sageUnfilteredVcf))
                .addCommand(new PipeCommands(new BcfToolsPipeableIncludeFilterCommand("'FILTER=\"PASS\"'", sageUnfilteredVcf),
                        new BcfToolsPipeableAnnotationCommand("INFO/HOTSPOT"),
                        new BcfToolsPipeableAnnotationCommand("FILTER/LOW_CONFIDENCE"),
                        new BcfToolsPipeableAnnotationCommand("FILTER/GERMLINE_INDEL"),
                        new BcfToolsPipeableViewCommand(tumorName, sageFilteredVcf)))
                .addCommand(new TabixCommand(sageFilteredVcf))
                .addCommand(new BcfToolsAnnotationCommand(newArrayList(sageResourceDownload.find("SAGE_PON.vcf.gz"),
                        "-c",
                        "SAGE_PON_COUNT"), sageFilteredVcf, sagePonVcf))
                .addCommand(new TabixCommand(sagePonVcf))
                .addCommand(new BcfToolsExcludeFilterCommand("'SAGE_PON_COUNT!=\".\" && MIN(SAGE_PON_COUNT) > 0'",
                        "SAGE_PON",
                        sagePonVcf,
                        sagePonFilteredVcf))
                .addCommand(new TabixCommand(sagePonFilteredVcf))
                .addCommand(new SageAnnotationCommand(filteredVcf, sagePonFilteredVcf, sageResourceDownload.find("tsv"), sageOutputVcf));
        return sageOutputVcf;
    }

    private static String filter(final BashStartupScript strelkaBash, final String postProcessedVcf) {
        String germlinePonFilteredVcf = OUTPUT_DIRECTORY + "/germline.pon.filtered.vcf.gz";
        String somaticPonFilteredVcf = OUTPUT_DIRECTORY + "/somatic.pon.filtered.vcf.gz";
        strelkaBash.addCommand(new BcfToolsExcludeFilterCommand("'GERMLINE_PON_COUNT!= \".\" && MIN(GERMLINE_PON_COUNT) > 5'",
                "GERMLINE_PON",
                postProcessedVcf,
                germlinePonFilteredVcf));
        strelkaBash.addCommand(new BcfToolsExcludeFilterCommand("'SOMATIC_PON_COUNT!=\".\" && MIN(SOMATIC_PON_COUNT) > 3'",
                "SOMATIC_PON",
                germlinePonFilteredVcf,
                somaticPonFilteredVcf));
        strelkaBash.addCommand(new TabixCommand(somaticPonFilteredVcf));
        return somaticPonFilteredVcf;
    }

    private static String strelkaPostProcess(final Storage storage, final AlignmentPair pair, final RuntimeBucket runtimeBucket,
            final BashStartupScript strelkaBash, final InputDownload tumorBam, final String annotatedVcf) {
        ResourceDownload bedDownload = new ResourceDownload(new Resource(storage, "beds", "beds").copyInto(runtimeBucket), runtimeBucket);
        String postProcessedVcf = OUTPUT_DIRECTORY + "/strelka-post-processed.vcf.gz";
        strelkaBash.addCommand(bedDownload)
                .addCommand(new StrelkaPostProcessCommand(annotatedVcf,
                        postProcessedVcf,
                        bedDownload.find("bed"),
                        pair.tumor().sample().name(),
                        tumorBam.getLocalTargetPath()));
        return postProcessedVcf;
    }

    private static String runStrelkaAndCombineVcfs(final InputDownload downloadReferenceBam, final InputDownload downloadReferenceBai,
            final InputDownload downloadTumorBam, final InputDownload downloadTumorBai, final ResourceDownload referenceGenomeDownload,
            final String referenceGenomeFile, final ResourceDownload configDownload, final BashStartupScript strelkaBash) {
        String combinedVcf = OUTPUT_DIRECTORY + "/combined.vcf";
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
                        combinedVcf));
        return combinedVcf;
    }

    private static String applyAnnotations(final Storage storage, final RuntimeBucket runtimeBucket, final String combinedVcfOutput,
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
        String somaticAnnotatedVcf = OUTPUT_DIRECTORY + "/somatic.pon.annotated.vcf.gz";
        annotate(SomaticCaller::somaticPonAnnotation, germlinePonAnnotatedVcf, somaticAnnotatedVcf, strelkaBash, ponDownload);
        return somaticAnnotatedVcf;
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