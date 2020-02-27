package com.hartwig.pipeline.resource;

public class Hg38Resource implements Resource
{
    public RefGenomeVersion version() { return RefGenomeVersion.HG38; }

    public String refGenomeFile() { return "Homo_sapiens_assembly38.fasta"; }

    public String gcProfileFile() { return "GC_profile.hg38.1000bp.cnp"; }

    public String germlineHetPon() { return "GermlineHetPon.hg38.vcf.gz"; }

    public String gridssRepeatMaskerDb() { return "hg38.fa.out"; }

    public String snpEffDb() { return "snpEff_v4_3_GRCh38.92.zip"; }


}
