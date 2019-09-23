package com.hartwig.pipeline.calling.somatic;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.TabixSubStageTest;
import org.junit.Test;

import static com.hartwig.pipeline.testsupport.TestConstants.*;
import static org.assertj.core.api.Assertions.assertThat;

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
        assertThat(output.currentBash().asUnixString()).contains(TOOLS_BCFTOOLS + " filter -e "
                + "'GERMLINE_PON_COUNT!= \".\" && MIN(GERMLINE_PON_COUNT) > 5' -s GERMLINE_PON -m+ " + outFile("tumor.strelka.vcf") + " -O u | "
                + TOOLS_BCFTOOLS + " filter -e 'SOMATIC_PON_COUNT!=\".\" && MIN(SOMATIC_PON_COUNT) > 3' -s SOMATIC_PON "
                + "-m+  -O z -o " + expectedPath() + ") >>" + LOG_FILE + " 2>&1 || die\n");
    }
}