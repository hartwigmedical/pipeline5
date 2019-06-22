package com.hartwig.pipeline.resource;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class GATKDictAliasTest {

    @Test
    public void removesFastaExtensionFromDictFile() {
        GATKDictAlias victim = new GATKDictAlias();
        assertThat(victim.apply("/path/to/referenceSampleName.fasta.dict")).isEqualTo("/path/to/referenceSampleName.dict");
    }

    @Test
    public void passThroughOtherFiles() {
        GATKDictAlias victim = new GATKDictAlias();
        String fileName = "/path/to/referenceSampleName.fasta.bwt";
        assertThat(victim.apply(fileName)).isEqualTo(fileName);
    }
}