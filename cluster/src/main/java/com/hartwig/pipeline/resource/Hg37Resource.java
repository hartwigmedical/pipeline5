package com.hartwig.pipeline.resource;

public class Hg37Resource implements Resource
{
    public RefGenomeVersion version() { return RefGenomeVersion.HG37; }

    public String refGenomeFile() { return "Homo_sapiens.GRCh37.GATK.illumina.fasta"; }
}
