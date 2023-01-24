package com.hartwig.pipeline.alignment.bwa;

import static java.lang.String.format;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class BamReheaderCommandTest {
    @Test
    public void createsValidCommand() {
        String inputBam = "/some/input/file.bam";
        assertThat(new BamReheaderCommand(inputBam).asBash()).isEqualTo(format("/opt/tools/samtools/1.14/samtools reheader --no-PG --command 'grep -v ^@PG' %s", inputBam));
    }
}