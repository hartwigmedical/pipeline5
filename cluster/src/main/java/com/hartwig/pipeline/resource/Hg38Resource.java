package com.hartwig.pipeline.resource;

public class Hg38Resource implements Resource
{
    public RefGenomeVersion version() { return RefGenomeVersion.HG38; }

    public String refGenomeFile() { return "Homo_sapiens.GRCh37.GATK.illumina.fasta"; }

}
