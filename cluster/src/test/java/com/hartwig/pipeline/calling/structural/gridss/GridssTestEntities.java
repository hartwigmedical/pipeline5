package com.hartwig.pipeline.calling.structural.gridss;

import static java.lang.String.format;

import java.util.Map;

import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.testsupport.CommonTestEntities;

public interface GridssTestEntities extends CommonTestEntities {
    String ARG_KEY_INPUT = "input";
    String ARG_KEY_INPUT_SHORT = "i";
    String ARG_KEY_OUTPUT = "output";
    String ARG_KEY_OUTPUT_SHORT = "o";
    String ARG_KEY_WORKING_DIR = "working_dir";

    String BLACKLIST = format("%s/ENCFF001TDO.bed", VmDirectories.RESOURCES);
    String CONFIG_FILE = format("%s/gridss.properties", VmDirectories.RESOURCES);

    Map.Entry<String, String> ARGS_TMP_DIR = pair("tmp_dir", System.getProperty("java.io.tmpdir"));
    Map.Entry<String, String> ARGS_REFERENCE_SEQUENCE = pair("reference_sequence", REFERENCE_GENOME);
    Map.Entry<String, String> ARGS_OUTPUT_TO_STDOUT = pair(ARG_KEY_OUTPUT_SHORT, "/dev/stdout");
    Map.Entry<String, String> ARGS_NO_COMPRESSION = pair("compression_level", "0");
    Map.Entry<String, String> ARGS_WORKING_DIR = pair("working_dir", OUT_DIR);


    static Map.Entry<String, String> pair(String key, String value) {
        return new Map.Entry<String, String>() {
            @Override
            public String getKey() {
                return key;
            }

            @Override
            public String getValue() {
                return value;
            }

            @Override
            public String setValue(String s) {
                return null;
            }
        };
    }
}
