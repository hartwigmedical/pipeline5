package com.hartwig.pipeline.adam;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import com.hartwig.patient.ReferenceGenome;
import com.hartwig.support.hadoop.Hadoop;
import com.hartwig.support.test.Resources;

import org.jetbrains.annotations.NotNull;
import org.junit.Test;

public class IndexFilesTest {

    private static final String REFERENCE_GENOME_DIR = Resources.testResource("reference_genome");
    private static final String REFERENCE_GENOME_PATH = REFERENCE_GENOME_DIR + "/Homo_sapiens.GRCh37.GATK.illumina.chr22.fa";

    @Test
    public void findsAllExtensionsInDirectoryOfFasta() throws Exception {
        List<String> indexFiles = IndexFiles.resolve(Hadoop.localFilesystem(), ReferenceGenome.of(listStatusInput(REFERENCE_GENOME_PATH)));
        assertThat(indexFiles).contains(listStatusInput(REFERENCE_GENOME_PATH),
                listStatusOutput(REFERENCE_GENOME_PATH + ".amb"),
                listStatusOutput(REFERENCE_GENOME_PATH + ".ann"),
                listStatusOutput(REFERENCE_GENOME_PATH + ".bwt"),
                listStatusOutput(REFERENCE_GENOME_PATH + ".dict"),
                listStatusOutput(REFERENCE_GENOME_PATH + ".fai"),
                listStatusOutput(REFERENCE_GENOME_PATH + ".img"),
                listStatusOutput(REFERENCE_GENOME_PATH + ".pac"),
                listStatusOutput(REFERENCE_GENOME_PATH + ".sa"));
    }

    @NotNull
    private static String listStatusInput(final String path) {
        return "file://" + path;
    }

    @NotNull
    private static String listStatusOutput(final String path) {
        return "file:" + path;
    }
}