package com.hartwig.pipeline.testsupport;

import static java.lang.String.format;

import com.hartwig.pipeline.execution.vm.VmDirectories;

public interface CommonTestEntities {
    String OUT_DIR = "/data/output";
    String RESOURCE_DIR = "/data/resources";
    String TOOLS_DIR = "/opt/tools";
    String IN_DIR = "/data/input";
    String LOG_FILE = "/var/log/run.log";

    default String outFile(String filename) {
        return OUT_DIR + "/" + filename;
    }

    default String inFile(String filename) {
        return IN_DIR + "/" + filename;
    }

    // TODO specific tools as well? eg
    // sambamba()
    // bwa()
    // etc

    default String copyOutputToStorage(String destination) {
        return "gsutil -qm -o GSUtil:parallel_composite_upload_threshold=150M cp -r /data/output/ " + destination;
    }

    default String copyInputToLocal(String source, String destination) {
        return format("gsutil -qm cp -n %s /data/input/%s", source, destination);
    }

    String REFERENCE_SAMPLE = "sample12345678R";
    String TUMOR_SAMPLE = "sample12345678T";
    String REFERENCE_BAM = format("%s/%s.bam", IN_DIR, REFERENCE_SAMPLE);
    String TUMOR_BAM = format("%s/%s.bam", IN_DIR, TUMOR_SAMPLE);
    String JOINT_NAME = REFERENCE_SAMPLE + "_" + TUMOR_SAMPLE;
    String ASSEMBLY_BAM = format("%s/%s_%s.assemble.bam", OUT_DIR, REFERENCE_SAMPLE, TUMOR_SAMPLE);

    String OUTPUT_BAM = format("%s/output.bam", OUT_DIR);
    String REFERENCE_GENOME = format("%s/reference_genome.fasta", VmDirectories.RESOURCES);
    String PATH_TO_SAMBAMBA = format("%s/sambamba/0.6.8/sambamba", TOOLS_DIR);
}