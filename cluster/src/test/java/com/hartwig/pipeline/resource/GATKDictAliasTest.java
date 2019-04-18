package com.hartwig.pipeline.resource;


import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.resource.GATKDictAlias;

import org.junit.Test;

public class GATKDictAliasTest {

    @Test
    public void removesFastaExtensionFromDictFile() {
        GATKDictAlias victim = new GATKDictAlias();
        assertThat(victim.apply("/path/to/reference.fasta.dict")).isEqualTo("/path/to/reference.dict");
    }

    @Test
    public void passThroughOtherFiles() {
        GATKDictAlias victim = new GATKDictAlias();
        String fileName = "/path/to/reference.fasta.bwt";
        assertThat(victim.apply(fileName)).isEqualTo(fileName);
    }
}