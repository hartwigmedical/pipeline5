package com.hartwig.pipeline.calling.structural.gridss.stage;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.calling.SubStageTest;
import com.hartwig.pipeline.stages.SubStage;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Test;

public class GridssSomaticFilterTest extends SubStageTest {
    @Override
    public SubStage createVictim() {
        return new GridssSomaticFilter(TestInputs.REF_GENOME_37_RESOURCE_FILES, "SAMPLE_T", "SAMPLE_R", "/data/output/tumor.strelka.vcf");
    }

    @Override
    public String expectedPath() {
        return "/data/output/tumor.gripss.somatic.vcf.gz";
    }

    @Test
    public void expectedOutput() {
        assertThat(bash()).contains("java -Xmx24G -cp /opt/tools/gripss/1.11/gripss.jar com.hartwig.hmftools.gripss.GripssApplicationKt "
                + "-ref_genome /opt/resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta "
                + "-breakpoint_hotspot /opt/resources/knowledgebases/37/known_fusions.bedpe "
                + "-breakend_pon /opt/resources/gridss_pon/37/gridss_pon_single_breakend.hg19.bed "
                + "-breakpoint_pon /opt/resources/gridss_pon/37/gridss_pon_breakpoint.hg19.bedpe "
                + "-reference SAMPLE_R -tumor SAMPLE_T "
                + "-input_vcf /data/output/tumor.strelka.vcf "
                + "-output_vcf /data/output/tumor.gripss.somatic.vcf.gz"
        );
    }

}
