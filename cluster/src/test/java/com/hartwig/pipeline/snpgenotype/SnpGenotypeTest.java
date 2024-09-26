package com.hartwig.pipeline.snpgenotype;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.PipelineStatus;
import com.hartwig.pipeline.input.SingleSampleRunMetadata;
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
    protected void validatePersistedOutput(final SnpGenotypeOutput output) {
        assertThat(output).isEqualTo(SnpGenotypeOutput.builder().status(PipelineStatus.PERSISTED).build());
    }

    @Override
    protected Arguments createDisabledArguments() {
        return Arguments.testDefaultsBuilder().runSnpGenotyper(false).build();
    }

    @Override
    protected Stage<SnpGenotypeOutput, SingleSampleRunMetadata> createVictim() {
        return new SnpGenotype(TestInputs.REF_GENOME_37_RESOURCE_FILES, TestInputs.referenceAlignmentOutput());
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
        final ArrayList<String> commands = new ArrayList<>();
        commands.add("/usr/lib/jvm/jdk8u302-b08/jre/bin/java -Xmx20G -jar /opt/tools/gatk/3.8.0/GenomeAnalysisTK.jar -T UnifiedGenotyper -nct $(grep -c '^processor' /proc/cpuinfo) "
                + "--input_file /data/input/reference.bam -o /data/output/snp_genotype_output.vcf -L "
                + "/opt/resources/genotype_snps/37/26SNPtaq.vcf --reference_sequence "
                + "/opt/resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta --output_mode EMIT_ALL_SITES");
        commands.add("/usr/lib/jvm/jdk8u302-b08/jre/bin/java -Xmx20G -jar /opt/tools/gatk/3.8.0/GenomeAnalysisTK.jar -T UnifiedGenotyper -nct $(grep -c '^processor' /proc/cpuinfo) "
                + "--input_file /data/input/reference.bam -o /data/output/snp_genotype_output_mip.vcf -L "
                + "/opt/resources/genotype_snps/37/31SNPtaq.vcf --reference_sequence "
                + "/opt/resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta --output_mode EMIT_ALL_SITES");
        return commands;
    }

    @Override
    protected void validateOutput(final SnpGenotypeOutput output) {
        // nothing additional to validate
    }
}