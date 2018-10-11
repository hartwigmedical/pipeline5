package com.hartwig.pipeline.bootstrap;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.staticdata.ReferenceGenomeAlias;

import org.junit.Before;
import org.junit.Test;

public class ReferenceGenomeAliasTest {

    private ReferenceGenomeAlias victim;

    @Before
    public void setUp() throws Exception {
        victim = new ReferenceGenomeAlias();
    }

    @Test
    public void fastaExtensionAliased() {
        assertThat(victim.apply("some_reference_genome.fasta")).isEqualTo("reference.fasta");
    }

    @Test
    public void faExtensionAliased() {
        assertThat(victim.apply("some_reference_genome.fa")).isEqualTo("reference.fa");
    }

    @Test
    public void multipleExtensionsAliasedFasta() {
        assertThat(victim.apply("some_reference_genome.fasta.bwt")).isEqualTo("reference.fasta.bwt");
    }

    @Test
    public void multipleExtensionsAliasedFa() {
        assertThat(victim.apply("some_reference_genome.fa.bwt")).isEqualTo("reference.fa.bwt");
    }

    @Test(expected = IllegalArgumentException.class)
    public void unknownExtensionThrowsIllegalArgument() {
        assertThat(victim.apply("some_reference_genome.fastq"));
    }
}