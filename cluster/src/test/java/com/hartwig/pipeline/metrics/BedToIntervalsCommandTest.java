package com.hartwig.pipeline.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class BedToIntervalsCommandTest {

    @Test
    public void shouldConvertBedToIntervalFileWithPicard() {
        BedToIntervalsCommand victim = new BedToIntervalsCommand("input.bed", "input.interval_list", "reference.fna");
        assertThat(victim.asBash()).isEqualTo(
                "java -Xmx1G -cp /opt/tools/gridss/2.13.2/gridss.jar picard.cmdline.PicardCommandLine BedToIntervalList SEQUENCE_DICTIONARY=reference.dict INPUT=input.bed OUTPUT=input.interval_list");
    }
}