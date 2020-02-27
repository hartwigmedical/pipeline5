package com.hartwig.pipeline.resource;

import static com.hartwig.pipeline.resource.ResourceNames.AMBER_PON;
import static com.hartwig.pipeline.resource.ResourceNames.GC_PROFILE;
import static com.hartwig.pipeline.resource.ResourceNames.GRIDSS_REPEAT_MASKER_DB;
import static com.hartwig.pipeline.resource.ResourceNames.MAPPABILITY;
import static com.hartwig.pipeline.resource.ResourceNames.REFERENCE_GENOME;
import static com.hartwig.pipeline.resource.ResourceNames.SAGE;
import static com.hartwig.pipeline.resource.ResourceNames.SNPEFF;

public class Hg38Resource implements Resource
{
    public RefGenomeVersion version() { return RefGenomeVersion.HG38; }

    public String refGenomeFile() { return Resource.of(REFERENCE_GENOME, "Homo_sapiens_assembly38.fasta"); }

    public String gcProfileFile() { return Resource.of(GC_PROFILE, "GC_profile.hg38.1000bp.cnp"); }

    public String germlineHetPon() { return Resource.of(AMBER_PON, "GermlineHetPon.hg38.vcf.gz"); }

    public String gridssRepeatMaskerDb() { return Resource.of(GRIDSS_REPEAT_MASKER_DB,"hg38.fa.out"); }

    public String snpEffDb() { return Resource.of(SNPEFF,"snpEff_v4_3_GRCh38.92.zip"); }

    public String sageKnownHotspots() { return Resource.of(SAGE, "KnownHotspots.hg38.vcf.gz"); }

    public String sageActionableCodingPanel() { return "ActionableCodingPanel.hg38.bed.gz"; }

    public String out150Mappability() { return Resource.of(MAPPABILITY, "out_150_hg38.mappability.bed.gz"); }

    public String sageGermlinePon() { return Resource.of(SAGE, "SageGermlinePon.hg38.vcf.gz"); }

}
