package com.hartwig.pipeline.calling.germline.command;

import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;

public class SnpConfig {
    public String snpTypes() {
        String argKey = "-snpType";
        return format("%s SNP %s NO_VARIATION", argKey, argKey);
    }

    public String snpFilters() {
        Map<String, String> filters = new HashMap<String, String>();
        filters.put("SNP_LowQualityDepth", "QD < 2.0");
        filters.put("SNP_MappingQuality", "MQ < 40.0");
        filters.put("SNP_StrandBias", "FS > 60.0");
        filters.put("SNP_HaplotypeScoreHigh", "HaplotypeScore > 13.0");
        filters.put("SNP_MQRankSumLow", "MQRankSum < -12.5");
        filters.put("SNP_ReadPosRankSumLow", "ReadPosRankSum < -8.0");

        String allFilters = "";
        for (String filter: filters.keySet()) {
            allFilters += format(" -snpFilterName %s -snpFilterExpression \"%s\"", filter, filters.get(filter));
        }
        return allFilters.trim();
    }
}
