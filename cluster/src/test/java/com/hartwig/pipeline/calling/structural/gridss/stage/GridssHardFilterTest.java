package com.hartwig.pipeline.calling.structural.gridss.stage;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.calling.SubStageTest;
import com.hartwig.pipeline.stages.SubStage;

import org.junit.Test;

public class GridssHardFilterTest extends SubStageTest {
    @Override
    public SubStage createVictim() {
        return new GridssHardFilter();
    }

    @Override
    public String expectedPath() {
        return "/data/output/tumor.gripss.somatic.filtered.vcf.gz";
    }

    @Test
    public void expectedOutput() {
        assertThat(bash()).contains("java -Xmx24G -cp /opt/tools/gripss/1.12/gripss.jar com.hartwig.hmftools.gripsskt.GripssHardFilterApplicationKt "
                + "-input_vcf /data/output/tumor.strelka.vcf "
                + "-output_vcf /data/output/tumor.gripss.somatic.filtered.vcf.gz"
        );
    }

}
