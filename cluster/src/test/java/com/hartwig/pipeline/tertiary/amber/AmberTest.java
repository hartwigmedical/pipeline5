package com.hartwig.pipeline.tertiary.amber;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.List;

import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.resource.Hg37Resource;
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
        return new Amber(TestInputs.defaultPair(), TestInputs.HG37_RESOURCE);
    }

    @Override
    protected List<String> expectedCommands() {
        return Collections.singletonList("java -Xmx32G -cp /opt/tools/amber/3.2/amber.jar com.hartwig.hmftools.amber.AmberApplication "
                + "-reference reference -reference_bam /data/input/reference.bam -tumor tumor -tumor_bam /data/input/tumor.bam -output_dir "
                + "/data/output -threads $(grep -c '^processor' /proc/cpuinfo) -ref_genome "
                + "/opt/resources/reference_genome/Homo_sapiens.GRCh37.GATK.illumina.fasta "
                + "-loci /opt/resources/amber_pon/GermlineHetPon.hg19.vcf.gz");
    }

    @Override
    protected void validateOutput(final AmberOutput output) {
        assertThat(output.outputDirectory().bucket()).isEqualTo("run-reference-tumor-test/amber");
        assertThat(output.outputDirectory().path()).isEqualTo("results");
        assertThat(output.outputDirectory().isDirectory()).isTrue();
    }
}