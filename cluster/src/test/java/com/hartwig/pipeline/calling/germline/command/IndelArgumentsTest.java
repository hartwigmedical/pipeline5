package com.hartwig.pipeline.calling.germline.command;

import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class IndelArgumentsTest {
    @Test
    public void shouldReturnIndelTypes() {
        String indelTypes = new IndelArguments().indelTypes();
        assertThat(indelTypes).isNotEmpty();
        String argKey = "-indelType";
        List<String> tokens = asList(indelTypes.split(" +"));
        assertThat(tokens.size()).isEqualTo(4);
        assertThat(tokens.get(0)).isEqualTo(argKey);
        assertThat(tokens.get(2)).isEqualTo(argKey);

        List<String> remainingTokens = asList(tokens.get(1), tokens.get(3));
        assertThat(remainingTokens).containsOnly("INDEL", "MIXED");
    }

    @Test
    public void shouldReturnIndelFilters() {
        Map<String, String> pairs = new HashMap<>();
        pairs.put("INDEL_LowQualityDepth", "QD < 2.0");
        pairs.put("INDEL_StrandBias", "FS > 200.0");
        pairs.put("INDEL_ReadPosRankSumLow", "ReadPosRankSum < -20.0");


        String actual = new IndelArguments().indelFilters().trim();
        assertThat(actual).isNotEmpty();
        for (String key: pairs.keySet()) {
            String quartet = format("-indelFilterName %s -indelFilterExpression \"%s\"", key, pairs.get(key));
            assertThat(actual).isNotEmpty();
            actual = actual.replaceFirst(quartet, "");
        }
        assertThat(actual.trim()).isEmpty();
    }

}
