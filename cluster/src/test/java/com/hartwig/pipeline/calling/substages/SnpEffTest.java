package com.hartwig.pipeline.calling.substages;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.TabixSubStageTest;
import org.junit.Test;

import static com.hartwig.pipeline.testsupport.TestConstants.*;
import static org.assertj.core.api.Assertions.assertThat;

public class SnpEffTest extends TabixSubStageTest {

    @Override
    public SubStage createVictim() {
        return new SnpEff("snpeff.config");
    }

    @Override
    public String expectedPath() {
        return outFile("tumor.snpeff.annotated.vcf.gz");
    }

    @Test
    public void runsSnpEff() {
        assertThat(output.currentBash().asUnixString()).contains(TOOLS_SNPEFF_DIR + "/snpEff.sh " + TOOLS_SNPEFF_DIR + "/snpEff.jar "
                + "snpeff.config GRCh37.75 " + outFile("tumor.strelka.vcf") + " " + outFile("tumor.snpeff.annotated.vcf"));
    }

    @Test
    public void bgzipsOutput() {
        assertThat(output.currentBash()
                .asUnixString()).contains(TOOLS_BGZIP + " -f " + outFile("tumor.snpeff.annotated.vcf"));
    }
}