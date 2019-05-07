package com.hartwig.pipeline.calling.structural.gridss;

import static java.lang.String.format;

public interface TestConstants {
    String OUT_DIR = "/data/output";
    String RESOURCE_DIR = "/data/resources";
    String TOOLS_DIR = "/data/tools";

    String REF_GENOME = format("%s/reference_genome.fasta", RESOURCE_DIR);

    String PATH_TO_BWA = format("%s/bwa/0.7.17/bwa", TOOLS_DIR);

    String GRIDSS_CONFIG = format("%s/gridss.properties", OUT_DIR);
}
