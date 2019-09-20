package com.hartwig.pipeline.calling.somatic;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.TabixSubStageTest;

import org.junit.Test;

public class PonFilterTest extends TabixSubStageTest {

    @Override
    public SubStage createVictim() {
        return new PonFilter();
    }

    @Override
    public String expectedPath() {
        return outFile("tumor.pon.filtered.vcf.gz");
    }

    @Test
    public void runsTwoPipedBcfToolsFilterCommandInSubshell() {
        assertThat(output.currentBash().asUnixString()).contains("(/opt/tools/bcftools/1.3.1/bcftools filter -e "
                + "'GERMLINE_PON_COUNT!= \".\" && MIN(GERMLINE_PON_COUNT) > 5' -s GERMLINE_PON -m+ " + outFile("tumor.strelka.vcf") + " -O u | "
                + "/opt/tools/bcftools/1.3.1/bcftools filter -e 'SOMATIC_PON_COUNT!=\".\" && MIN(SOMATIC_PON_COUNT) > 3' -s SOMATIC_PON "
                + "-m+  -O z -o " + expectedPath() + ") >>" + LOG_FILE + " 2>&1 || die\n");
    }
}