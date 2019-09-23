package com.hartwig.pipeline.calling.somatic;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.TabixSubStageTest;
import org.junit.Test;

import static com.hartwig.pipeline.testsupport.TestConstants.TOOLS_BCFTOOLS;
import static com.hartwig.pipeline.testsupport.TestConstants.outFile;
import static org.assertj.core.api.Assertions.assertThat;

public class PonAnnotationTest extends TabixSubStageTest {

    @Override
    public SubStage createVictim() {
        return new PonAnnotation("germline.pon", "GERMLINE_PON.vcf.gz", "GERMLINE_PON_COUNT");
    }

    @Override
    public String expectedPath() {
        return outFile("tumor.germline.pon.annotated.vcf.gz");
    }

    @Test
    public void runsBcfToolsPonAnnotation() {
        assertThat(output.currentBash().asUnixString()).contains(TOOLS_BCFTOOLS + " annotate -a "
                + "GERMLINE_PON.vcf.gz -c GERMLINE_PON_COUNT -o " + expectedPath());
    }
}