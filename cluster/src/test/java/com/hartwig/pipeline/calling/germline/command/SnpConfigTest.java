package com.hartwig.pipeline.calling.germline.command;

import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class SnpConfigTest {
    @Test
    public void shouldReturnSnpTypes() {
        String snpTypes = new SnpConfig().snpTypes();
        String argKey = "-snpType";
        assertThat(snpTypes).isNotEmpty();
        List<String> tokens = asList(snpTypes.split(" +"));
        assertThat(tokens.size()).isEqualTo(4);
        assertThat(tokens.get(0)).isEqualTo(argKey);
        assertThat(tokens.get(2)).isEqualTo(argKey);

        List<String> remainingTokens = asList(tokens.get(1), tokens.get(3));
        assertThat(remainingTokens).containsOnly("SNP", "NO_VARIATION");
    }

    @Test
    public void shouldReturnSnpFilters() {
        Map<String, String> pairs = new HashMap<>();
        pairs.put("SNP_LowQualityDepth", "QD < 2.0");
        pairs.put("SNP_MappingQuality", "MQ < 40.0");
        pairs.put("SNP_StrandBias", "FS > 60.0");
        pairs.put("SNP_HaplotypeScoreHigh", "HaplotypeScore > 13.0");
        pairs.put("SNP_MQRankSumLow", "MQRankSum < -12.5");
        pairs.put("SNP_ReadPosRankSumLow", "ReadPosRankSum < -8.0");

        String actual = new SnpConfig().snpFilters().trim();
        for (String key: pairs.keySet()) {
            String quartet = format("-snpFilterName %s -snpFilterExpression \"%s\"", key, pairs.get(key));
            assertThat(actual).isNotEmpty();
            actual = actual.replaceFirst(quartet, "");
        }
        assertThat(actual.trim()).isEmpty();
    }
}
