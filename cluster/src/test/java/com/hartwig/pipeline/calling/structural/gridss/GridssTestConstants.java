package com.hartwig.pipeline.calling.structural.gridss;

import com.hartwig.pipeline.execution.vm.VmDirectories;

import java.util.Map;

import static com.hartwig.pipeline.testsupport.TestConstants.IN_DIR;
import static com.hartwig.pipeline.testsupport.TestConstants.OUT_DIR;
import static java.lang.String.format;

public class GridssTestConstants {
    public static final String ARG_KEY_INPUT = "input";
    public static final String ARG_KEY_INPUT_SHORT = "i";
    public static final String ARG_KEY_OUTPUT = "output";
    public static final String ARG_KEY_OUTPUT_SHORT = "o";
    public static final String ARG_KEY_WORKING_DIR = "working_dir";

    public static final String REFERENCE_SAMPLE = "sample12345678R";
    public static final String TUMOR_SAMPLE = "sample12345678T";
    public static final String REFERENCE_BAM = format("%s/%s.bam", IN_DIR, REFERENCE_SAMPLE);
    public static final String TUMOR_BAM = format("%s/%s.bam", IN_DIR, TUMOR_SAMPLE);
    public static final String JOINT_NAME = REFERENCE_SAMPLE + "_" + TUMOR_SAMPLE;
    public static final String ASSEMBLY_BAM = format("%s/%s_%s.assemble.bam", OUT_DIR, REFERENCE_SAMPLE, TUMOR_SAMPLE);

    public static final String OUTPUT_BAM = format("%s/output.bam", OUT_DIR);
    public static final String REFERENCE_GENOME = format("%s/reference_genome.fasta", VmDirectories.RESOURCES);

    public static final String BLACKLIST = format("%s/ENCFF001TDO.bed", VmDirectories.RESOURCES);
    public static final String CONFIG_FILE = format("%s/gridss.properties", VmDirectories.RESOURCES);

    public static final Map.Entry<String, String> ARGS_TMP_DIR = pair("tmp_dir", System.getProperty("java.io.tmpdir"));
    public static final Map.Entry<String, String> ARGS_REFERENCE_SEQUENCE = pair("reference_sequence", REFERENCE_GENOME);
    public static final Map.Entry<String, String> ARGS_OUTPUT_TO_STDOUT = pair(ARG_KEY_OUTPUT_SHORT, "/dev/stdout");
    public static final Map.Entry<String, String> ARGS_NO_COMPRESSION = pair("compression_level", "0");
    public static final Map.Entry<String, String> ARGS_WORKING_DIR = pair("working_dir", OUT_DIR);

    private static Map.Entry<String, String> pair(String key, String value) {
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
