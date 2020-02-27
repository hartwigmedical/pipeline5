package com.hartwig.pipeline.resource;

import static com.hartwig.pipeline.resource.ResourceNames.AMBER_PON;
import static com.hartwig.pipeline.resource.ResourceNames.GC_PROFILE;
import static com.hartwig.pipeline.resource.ResourceNames.GRIDSS_REPEAT_MASKER_DB;
import static com.hartwig.pipeline.resource.ResourceNames.REFERENCE_GENOME;
import static com.hartwig.pipeline.resource.ResourceNames.SNPEFF;

public class Hg37Resource implements Resource
{
    public RefGenomeVersion version() { return RefGenomeVersion.HG37; }

    public String refGenomeFile() { return Resource.of(REFERENCE_GENOME, "Homo_sapiens.GRCh37.GATK.illumina.fasta"); }

    public String gcProfileFile() { return Resource.of(GC_PROFILE,"GC_profile.1000bp.cnp"); }

    public String amberPon() { return Resource.of(AMBER_PON, "GermlineHetPon.hg19.vcf.gz"); }

    public String germlineHetPon() { return "GermlineHetPon.hg19.vcf.gz"; }

    public String gridssRepeatMaskerDb() { return Resource.of(GRIDSS_REPEAT_MASKER_DB,"hg19.fa.out"); }

    public String snpEffDb() { return Resource.of(SNPEFF,"snpEff_v4_3_GRCh37.75.zip"); }

}
