package com.hartwig.pipeline.calling.germline.command;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.TabixSubStageTest;
import org.junit.Test;

import static com.hartwig.pipeline.testsupport.TestConstants.TOOLS_SNPEFF_DIR;
import static com.hartwig.pipeline.testsupport.TestConstants.outFile;
import static org.assertj.core.api.Assertions.assertThat;

public class SnpSiftFrequenciesAnnotationTest extends TabixSubStageTest {

    @Override
    public SubStage createVictim() {
        return new SnpSiftFrequenciesAnnotation("gonl_v5.vcf.gz", "snpEff.config");
    }

    @Override
    public String expectedPath() {
        return outFile("tumor.gonlv5.annotated.vcf.gz");
    }

    @Test
    public void runsSnpSiftFrequenciesAnnotation() {
        assertThat(output.currentBash().asUnixString()).contains("(java -Xmx20G -jar " + TOOLS_SNPEFF_DIR + "/SnpSift.jar annotate -c "
                + "snpEff.config -tabix -name GoNLv5_ -info AF,AN,AC gonl_v5.vcf.gz " + outFile("tumor.strelka.vcf") + " > "
                + outFile("tumor.gonlv5.annotated.vcf") + ")");
    }

    @Test
    public void runsBgZip() {
        assertThat(output.currentBash().asUnixString()).contains("bgzip -f " + outFile("tumor.gonlv5.annotated.vcf"));
    }
}