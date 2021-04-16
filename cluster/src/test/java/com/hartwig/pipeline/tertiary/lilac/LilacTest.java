package com.hartwig.pipeline.tertiary.lilac;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import com.google.common.collect.Lists;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.tertiary.TertiaryStageTest;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Before;

public class LilacTest extends TertiaryStageTest<LilacOutput> {

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected boolean isEnabledOnShallowSeq() {
        return false;
    }

    @Override
    public void enabledAppropriately() {
        assertThat(victim.shouldRun(Arguments.testDefaultsBuilder().runHlaTyping(true).build())).isTrue();
    }

    @Override
    protected Stage<LilacOutput, SomaticRunMetadata> createVictim() {
        return new Lilac(TestInputs.defaultPair(), TestInputs.REF_GENOME_37_RESOURCE_FILES, TestInputs.purpleOutput());
    }

    @Override
    protected List<String> expectedCommands() {
        return List.of("java -Xmx15G -cp /opt/tools/lilac/0.1/lilac.jar com.hartwig.hmftools.lilac.LilacApplicationKt "
                        + "-ref_genome /opt/resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta "
                        + "-sample tumor -reference_bam /data/input/reference.bam -tumor_bam /data/input/tumor.bam "
                        + "-output_dir /data/output -resource_dir /opt/resources/lilac/ "
                        + "-gene_copy_number /data/input/tumor.purple.cnv.gene.tsv -somatic_vcf /data/input/tumor.purple.somatic.vcf.gz "
                        + "-threads $(grep -c '^processor' /proc/cpuinfo)");
    }

    @Override
    protected void validateOutput(final LilacOutput output) {
        assertThat(output.outputDirectory().bucket()).isEqualTo("run-reference-tumor-test/lilac");
        assertThat(output.outputDirectory().path()).isEqualTo("results");
        assertThat(output.outputDirectory().isDirectory()).isTrue();
    }

    @Override
    protected List<String> expectedInputs() {
        List<String> result = Lists.newArrayList();
        result.addAll(super.expectedInputs());
        result.add(input("run-reference-tumor-test/purple/tumor.purple.cnv.gene.tsv", "tumor.purple.cnv.gene.tsv"));
        result.add(input("run-reference-tumor-test/purple/tumor.purple.somatic.vcf.gz", "tumor.purple.somatic.vcf.gz"));
        result.add(input("run-reference-tumor-test/purple/tumor.purple.somatic.vcf.gz.tbi", "tumor.purple.somatic.vcf.gz.tbi"));

        return result;
    }
}
