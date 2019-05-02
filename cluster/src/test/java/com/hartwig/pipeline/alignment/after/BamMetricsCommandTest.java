package com.hartwig.pipeline.alignment.after;

import com.hartwig.patient.Sample;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BamMetricsCommandTest {
    private static final String OPERATION = "CollectWgsMetrics";
    private String actual;
    private String reference;
    private String input;
    private String sampleName;
    private Sample sample;

    @Before
    public void setup() {
        reference = "reference.fasta";
        input = "input.bam";
        sampleName = "sample";
        sample = mock(Sample.class);

        when(sample.name()).thenReturn(sampleName);

        actual = new BamMetricsCommand("input.bam", "reference.fasta", sample).asBash();
    }

    @Test
    public void shouldStartCommandLineWithJarAndOperation() {
        assertThat(actual).startsWith("java -Xmx24G -jar /data/tools/picard/2.18.27/picard.jar " + OPERATION);
    }

    @Test
    public void shouldCompleteCommandLineWithOptions() {
        String[] halves = actual.split(OPERATION);
        assertThat(halves).isNotNull();
        assertThat(halves.length).isEqualTo(2);

        List<String> secondHalf = Arrays.asList(halves[1].trim().split(" +"));

        Map<String, String> options = new HashMap<>();
        options.put("REFERENCE_SEQUENCE", reference);
        options.put("INPUT", input);
        options.put("OUTPUT", format("/data/output/%s.wgsmetrics", sampleName));
        options.put("MINIMUM_MAPPING_QUALITY", "20");
        options.put("MINIMUM_BASE_QUALITY", "10");
        options.put("COVERAGE_CAP", "250");

        assertThat(secondHalf.size()).isEqualTo(options.size());
        for (String key: options.keySet()) {
            assertThat(secondHalf.contains(format("%s=%s", key, options.get(key)))).isTrue();
        }
    }
}