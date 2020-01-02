package com.hartwig.bcl2fastq.qc;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collection;
import java.util.Collections;

import com.google.common.collect.ImmutableList;
import com.hartwig.bcl2fastq.stats.Stats;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class QualityControlTest {

    @Parameterized.Parameter(0)
    public String statsLocation;

    @Parameterized.Parameters(name = "{index}: Test with [{0}]")
    public static Collection<Object[]> data() {
        return ImmutableList.of(new Object[] { "iseq/Stats.json" },
                new Object[] { "nextseq/Stats.json" },
                new Object[] { "novaseq/Stats.json" });
    }

    @Test
    public void parsesStatsJsonAndPassesToQCChecks() {
        QualityControl victim = new QualityControl(Collections.singletonList((stats, log) -> QualityControlResult.of("test", true)),
                Collections.emptyList(),
                Collections.emptyList());
        assertThat(victim.evaluate(Stats.builder().flowcell("test").build(), "").flowcellLevel()).allMatch(QualityControlResult::pass);
    }
}