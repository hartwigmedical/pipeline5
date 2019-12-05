package com.hartwig.pipeline.tertiary.amber;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.List;

import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.tertiary.TertiaryStageTest;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Before;

public class AmberTest extends TertiaryStageTest<AmberOutput> {

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected Stage<AmberOutput, SomaticRunMetadata> createVictim() {
        return new Amber(TestInputs.defaultPair());
    }

    @Override
    protected List<String> expectedCommands() {
        return Collections.singletonList("java -Xmx32G -cp /opt/tools/amber/2.5/amber.jar com.hartwig.hmftools.amber.AmberApplication "
                + "-reference reference -reference_bam /data/input/reference.bam -tumor tumor -tumor_bam /data/input/tumor.bam -output_dir "
                + "/data/output -threads 16 -ref_genome /opt/resources/reference_genome/Homo_sapiens.GRCh37.GATK.illumina.fasta -bed "
                + "/opt/resources/amber_pon/GermlineHetPon.hg19.bed -snp_bed /opt/resources/amber_pon/GermlineSnp.hg19.bed -threads "
                + "$(grep -c '^processor' /proc/cpuinfo)");
    }

    @Override
    protected void validateOutput(final AmberOutput output) {
        assertThat(output.outputDirectory().bucket()).isEqualTo("run-reference-tumor-test/amber");
        assertThat(output.outputDirectory().path()).isEqualTo("results");
        assertThat(output.outputDirectory().isDirectory()).isTrue();
    }
}