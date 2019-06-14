package com.hartwig.pipeline.calling.structural.gridss;

import com.hartwig.pipeline.execution.vm.VmDirectories;

import java.util.Map;

import static java.lang.String.format;

public interface CommonEntities {
    String OUT_DIR = "/data/output";
    String RESOURCE_DIR = "/data/resources";
    String TOOLS_DIR = "/data/tools";
    String IN_DIR = "/data/input";

    String REFERENCE_SAMPLE = "sample12345678R";
    String TUMOR_SAMPLE = "sample12345678T";
    String REFERENCE_BAM = format("%s/%s.bam", IN_DIR, REFERENCE_SAMPLE);
    String TUMOR_BAM = format("%s/%s.bam", IN_DIR, TUMOR_SAMPLE);

    String OUTPUT_BAM = format("%s/output.bam", OUT_DIR);
    String REFERENCE_GENOME = format("%s/reference_genome.fasta", VmDirectories.RESOURCES);
    String BLACKLIST = format("%s/ENCFF001TDO.bed", VmDirectories.RESOURCES);
    String GRIDSS_CONFIG = format("%s/gridss.properties", VmDirectories.RESOURCES);

    String PATH_TO_BWA = format("%s/bwa/0.7.17/bwa", TOOLS_DIR);
    String PATH_TO_SAMTOOLS = format("%s/samtools/1.2/samtools", TOOLS_DIR);
    String PATH_TO_SAMBAMBA = format("%s/sambamba/0.6.5/sambamba", TOOLS_DIR);

    String ARG_KEY_INPUT = "input";
    String ARG_KEY_INPUT_SHORT = "i";
    String ARG_KEY_OUTPUT = "output";
    String ARG_KEY_OUTPUT_SHORT = "o";
    String ARG_KEY_WORKING_DIR = "working_dir";

    Map.Entry<String, String> ARGS_TMP_DIR = pair("tmp_dir", "/tmp");
    Map.Entry<String, String> ARGS_REFERENCE_SEQUENCE = pair("reference_sequence", REFERENCE_GENOME);
    Map.Entry<String, String> ARGS_OUTPUT_TO_STDOUT = pair(ARG_KEY_OUTPUT_SHORT, "/dev/stdout");
    Map.Entry<String, String> ARGS_NO_COMPRESSION = pair("compression_level", "0");
    Map.Entry<String, String> ARGS_BLACKLIST = pair("blacklist", BLACKLIST);
    Map.Entry<String, String> ARGS_GRIDSS_CONFIG = pair("configuration_file", GRIDSS_CONFIG);
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