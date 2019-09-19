package com.hartwig.pipeline.calling.somatic;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.SubStageTest;

import org.junit.Test;

public class StrelkaPostProcessTest extends SubStageTest {

    @Override
    public SubStage createVictim() {
        return new StrelkaPostProcess("tumor", "NA12878.bed", "tumor.bam");
    }

    @Override
    public String expectedPath() {
        return "/data/output/tumor.strelka.post.processed.vcf.gz";
    }

    @Test
    public void runsStrelkaPostProcessor() {
        assertThat(output.currentBash().asUnixString()).contains("java -Xmx20G -jar "
                + "/opt/tools/strelka-post-process/1.4/strelka-post-process.jar -v /data/output/tumor.strelka.vcf -hc_bed NA12878.bed -t "
                + "tumor -o /data/output/tumor.strelka.post.processed.vcf.gz -b tumor.bam");
    }
}