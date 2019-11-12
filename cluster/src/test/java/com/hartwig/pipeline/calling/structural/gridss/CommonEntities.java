package com.hartwig.pipeline.calling.structural.gridss;

import static java.lang.String.format;

import java.util.Map;

import com.hartwig.pipeline.execution.vm.VmDirectories;

public interface CommonEntities {
    String OUT_DIR = "/data/output";
    String RESOURCE_DIR = "/data/resources";
    String TOOLS_DIR = "/opt/tools";
    String IN_DIR = "/data/input";
    String LOG_FILE = "/var/log/run.log";

    String REFERENCE_SAMPLE = "sample12345678R";
    String TUMOR_SAMPLE = "sample12345678T";
    String REFERENCE_BAM = format("%s/%s.bam", IN_DIR, REFERENCE_SAMPLE);
    String TUMOR_BAM = format("%s/%s.bam", IN_DIR, TUMOR_SAMPLE);
    String JOINT_NAME = REFERENCE_SAMPLE + "_" + TUMOR_SAMPLE;
    String ASSEMBLY_BAM = format("%s/%s_%s.assemble.bam", OUT_DIR, REFERENCE_SAMPLE, TUMOR_SAMPLE);

    String OUTPUT_BAM = format("%s/output.bam", OUT_DIR);
    String REFERENCE_GENOME = format("%s/reference_genome.fasta", VmDirectories.RESOURCES);
    String BLACKLIST = format("%s/ENCFF001TDO.bed", VmDirectories.RESOURCES);
    String CONFIG_FILE = format("%s/gridss.properties", VmDirectories.RESOURCES);

    String PATH_TO_SAMBAMBA = format("%s/sambamba/0.6.8/sambamba", TOOLS_DIR);

    String ARG_KEY_INPUT = "input";
    String ARG_KEY_INPUT_SHORT = "i";
    String ARG_KEY_OUTPUT = "output";
    String ARG_KEY_OUTPUT_SHORT = "o";
    String ARG_KEY_WORKING_DIR = "working_dir";

    Map.Entry<String, String> ARGS_TMP_DIR = pair("tmp_dir", System.getProperty("java.io.tmpdir"));
    Map.Entry<String, String> ARGS_REFERENCE_SEQUENCE = pair("reference_sequence", REFERENCE_GENOME);
    Map.Entry<String, String> ARGS_OUTPUT_TO_STDOUT = pair(ARG_KEY_OUTPUT_SHORT, "/dev/stdout");
    Map.Entry<String, String> ARGS_NO_COMPRESSION = pair("compression_level", "0");
    Map.Entry<String, String> ARGS_BLACKLIST = pair("blacklist", BLACKLIST);
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