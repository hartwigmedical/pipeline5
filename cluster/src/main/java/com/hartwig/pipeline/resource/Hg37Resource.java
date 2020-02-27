package com.hartwig.pipeline.resource;

public class Hg37Resource implements Resource
{
    public RefGenomeVersion version() { return RefGenomeVersion.HG37; }

    public String refGenomeFile() { return "Homo_sapiens.GRCh37.GATK.illumina.fasta"; }

    public String gcProfileFile() { return "GC_profile.1000bp.cnp"; }

    public String germlineHetPon() { return "GermlineHetPon.hg19.vcf.gz"; }

    public String gridssRepeatMaskerDb() { return "hg19.fa.out"; }

    public String snpEffDb() { return "snpEff_v4_3_GRCh37.75.zip"; }

}
