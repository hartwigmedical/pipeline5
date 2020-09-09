package com.hartwig.pipeline.calling.structural.gridss.stage;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.SubStageTest;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Test;

public class GridssSomaticFilterTest extends SubStageTest {
    @Override
    public SubStage createVictim() {
        return new GridssSomaticFilter(TestInputs.HG19_RESOURCE_FILES, "/data/output/tumor.strelka.vcf");
    }

    @Override
    public String expectedPath() {
        return "/data/output/tumor.gripss.somatic.vcf.gz";
    }

    @Test
    public void expectedOutput() {
        assertThat(bash()).contains("java -Xmx24G -cp /opt/tools/gripss/1.7/gripss.jar com.hartwig.hmftools.gripss.GripssApplicationKt "
                + "-ref_genome /opt/resources/reference_genome/hg19/Homo_sapiens.GRCh37.GATK.illumina.fasta "
                + "-breakpoint_hotspot /opt/resources/knowledgebases/hg19/KnownFusionPairs.hg19.bedpe "
                + "-breakend_pon /opt/resources/gridss_pon/hg19/gridss_pon_single_breakend.hg19.bed "
                + "-breakpoint_pon /opt/resources/gridss_pon/hg19/gridss_pon_breakpoint.hg19.bedpe "
                + "-input_vcf /data/output/tumor.strelka.vcf "
                + "-output_vcf /data/output/tumor.gripss.somatic.vcf.gz"
        );
    }

}
