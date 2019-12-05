package com.hartwig.bcl2fastq.qc;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.FileInputStream;
import java.util.Collections;

import com.hartwig.pipeline.testsupport.Resources;

import org.junit.Test;

public class QualityControlTest {

    @Test
    public void checksUnderminedYieldPercentage() throws Exception {
        QualityControl victim = new QualityControl(Collections.singletonList((stats, log) -> QualityControlResult.of("test", true)),
                Collections.emptyList(),
                Collections.emptyList());
        assertThat(victim.evaluate(new String(new FileInputStream(Resources.testResource("Stats.json")).readAllBytes()), "")
                .flowcellLevel()).allMatch(QualityControlResult::pass);
    }
}