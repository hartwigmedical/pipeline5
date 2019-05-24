package com.hartwig.pipeline.calling.germline.command;

import java.util.HashMap;
import java.util.Map;

public class IndelArguments {
    public String indelTypes() {
        String argKey = "-indelType";
        return String.format("%s INDEL %s MIXED", argKey, argKey);
    }

    public String indelFilters() {
        Map<String, String> arguments = new HashMap<>();
        arguments.put("INDEL_LowQualityDepth", "QD < 2.0");
        arguments.put("INDEL_StrandBias", "FS > 200.0");
        arguments.put("INDEL_ReadPosRankSumLow", "ReadPosRankSum < -20.0");

        String filters = "";
        for (String key: arguments.keySet()) {
            filters += String.format("-indelFilterName %s -indelFilterExpression \"%s\"", key, arguments.get(key));
        }
        return filters;
    }
}
