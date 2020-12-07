package com.hartwig.pipeline.snpgenotype;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.stages.StageTest;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Before;

public class SnpGenotypeTest extends StageTest<SnpGenotypeOutput, SingleSampleRunMetadata> {

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected Arguments createDisabledArguments() {
        return Arguments.testDefaultsBuilder().runSnpGenotyper(false).build();
    }

    @Override
    protected Stage<SnpGenotypeOutput, SingleSampleRunMetadata> createVictim() {
        return new SnpGenotype(TestInputs.HG37_RESOURCE_FILES, TestInputs.referenceAlignmentOutput());
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
    protected String expectedRuntimeBucketName() {
        return "run-reference-test";
    }

    @Override
    protected List<String> expectedCommands() {
        return Collections.singletonList(
                "java -Xmx20G -jar /opt/tools/gatk/3.8.0/GenomeAnalysisTK.jar -T UnifiedGenotyper -nct $(grep -c '^processor' /proc/cpuinfo) "
                        + "--input_file /data/input/reference.bam -o /data/output/snp_genotype_output.vcf -L "
                        + "/opt/resources/genotype_snps/hg37/26SNPtaq.vcf --reference_sequence "
                        + "/opt/resources/reference_genome/hg37/Homo_sapiens.GRCh37.GATK.illumina.fasta --output_mode EMIT_ALL_SITES");
    }

    @Override
    protected void validateOutput(final SnpGenotypeOutput output) {
        // nothing additional to validate
    }

    @Override
    protected void validatePersistedOutput(final SnpGenotypeOutput output) {
        assertThat(output).isEqualTo(SnpGenotypeOutput.builder().status(PipelineStatus.PERSISTED).build());
    }
}