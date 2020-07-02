package com.hartwig.pipeline.calling.structural.gridss.stage;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.SubStageTest;

import org.junit.Test;

public class GridssPassAndPonFilterTest extends SubStageTest {
    @Override
    public SubStage createVictim() {
        return new GridssPassAndPonFilter();
    }

    @Override
    public String expectedPath() {
        return "/data/output/tumor.gridss.somatic.filtered.vcf.gz";
    }

    @Test
    public void expectedOutput() {
        assertThat(bash()).contains(
                "gunzip -c /data/output/tumor.strelka.vcf | awk '$7 == \"PASS\" || $7 == \"PON\" || $1 ~ /^#/ ' | /opt/tools/tabix/0.2.6/bgzip  | tee /data/output/tumor.gridss.somatic.filtered.vcf.gz > /dev/null");
    }


}
