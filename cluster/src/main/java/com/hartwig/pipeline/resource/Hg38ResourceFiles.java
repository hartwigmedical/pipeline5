package com.hartwig.pipeline.resource;

import static com.hartwig.pipeline.resource.ResourceNames.AMBER_PON;
import static com.hartwig.pipeline.resource.ResourceNames.GC_PROFILE;
import static com.hartwig.pipeline.resource.ResourceNames.GRIDSS_REPEAT_MASKER_DB;
import static com.hartwig.pipeline.resource.ResourceNames.MAPPABILITY;
import static com.hartwig.pipeline.resource.ResourceNames.REFERENCE_GENOME;
import static com.hartwig.pipeline.resource.ResourceNames.SAGE;
import static com.hartwig.pipeline.resource.ResourceNames.SNPEFF;

public class Hg38ResourceFiles implements ResourceFiles
{
    public RefGenomeVersion version() { return RefGenomeVersion.HG38; }

    public String refGenomeFile() { return ResourceFiles.of(REFERENCE_GENOME, "Homo_sapiens_assembly38.fasta"); }

    public String gcProfileFile() { return ResourceFiles.of(GC_PROFILE, "GC_profile.hg38.1000bp.cnp"); }

    public String germlineHetPon() { return ResourceFiles.of(AMBER_PON, "GermlineHetPon.hg38.vcf.gz"); }

    public String gridssRepeatMaskerDb() { return ResourceFiles.of(GRIDSS_REPEAT_MASKER_DB,"hg38.fa.out"); }

    public String snpEffDb() { return ResourceFiles.of(SNPEFF,"snpEff_v4_3_GRCh38.92.zip"); }

    public String sageKnownHotspots() { return ResourceFiles.of(SAGE, "KnownHotspots.hg38.vcf.gz"); }

    public String sageActionableCodingPanel() { return "ActionableCodingPanel.hg38.bed.gz"; }

    public String out150Mappability() { return ResourceFiles.of(MAPPABILITY, "out_150_hg38.mappability.bed.gz"); }

    public String sageGermlinePon() { return ResourceFiles.of(SAGE, "SageGermlinePon.hg38.vcf.gz"); }

}
