package com.hartwig.pipeline.snpgenotype;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.resource.ResourceNames;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.stages.StageTest;
import com.hartwig.pipeline.testsupport.MockResource;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Before;

public class SnpGenotypeTest extends StageTest<SnpGenotypeOutput, SingleSampleRunMetadata> {

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockResource.addToStorage(storage, ResourceNames.REFERENCE_GENOME, "reference.fasta");
        MockResource.addToStorage(storage, ResourceNames.GENOTYPE_SNPS, "26SNPtaq.vcf");
    }

    @Override
    protected Arguments createDisabledArguments() {
        return Arguments.testDefaultsBuilder().runSnpGenotyper(false).build();
    }

    @Override
    protected Stage<SnpGenotypeOutput, SingleSampleRunMetadata> createVictim() {
        return new SnpGenotype(TestInputs.referenceAlignmentOutput());
    }

    @Override
    protected SingleSampleRunMetadata input() {
        return TestInputs.referenceRunMetadata();
    }

    @Override
    protected List<String> expectedInputs() {
        return ImmutableList.of(input("run-reference-test/aligner/results/reference.bam", "reference.bam"),
                input("run-reference-test/aligner/results/reference.bam.bai", "reference.bam.bai"));
    }

    @Override
    protected List<String> expectedResources() {
        return ImmutableList.of(resource(ResourceNames.REFERENCE_GENOME), resource(ResourceNames.GENOTYPE_SNPS));
    }

    @Override
    protected String expectedRuntimeBucketName() {
        return "run-reference-test";
    }

    @Override
    protected List<String> expectedCommands() {
        return Collections.singletonList(
                "java -Xmx20G -jar $TOOLS_DIR/gatk/3.8.0/GenomeAnalysisTK.jar -T UnifiedGenotyper -nct $(grep -c '^processor' /proc/cpuinfo) "
                        + "--input_file /data/input/reference.bam -o /data/output/snp_genotype_output.vcf -L /data/resources/26SNPtaq.vcf "
                        + "--reference_sequence /data/resources/reference.fasta --output_mode EMIT_ALL_SITES");
    }

    @Override
    protected void validateOutput(final SnpGenotypeOutput output) {
        // nothing additional to validate
    }
}