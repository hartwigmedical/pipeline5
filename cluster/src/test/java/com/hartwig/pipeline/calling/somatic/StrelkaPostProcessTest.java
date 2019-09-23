package com.hartwig.pipeline.calling.somatic;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.SubStageTest;
import org.junit.Test;

import static com.hartwig.pipeline.testsupport.TestConstants.TOOLS_STRELKA_POSTPROCESS;
import static com.hartwig.pipeline.testsupport.TestConstants.outFile;
import static org.assertj.core.api.Assertions.assertThat;

public class StrelkaPostProcessTest extends SubStageTest {

    @Override
    public SubStage createVictim() {
        return new StrelkaPostProcess("tumor", "NA12878.bed", "tumor.bam");
    }

    @Override
    public String expectedPath() {
        return outFile("tumor.strelka.post.processed.vcf.gz");
    }

    @Test
    public void runsStrelkaPostProcessor() {
        assertThat(output.currentBash().asUnixString()).contains("java -Xmx20G -jar "
                + TOOLS_STRELKA_POSTPROCESS + " -v " + outFile("tumor.strelka.vcf") + " -hc_bed NA12878.bed -t "
                + "tumor -o " + expectedPath() + " -b tumor.bam");
    }
}