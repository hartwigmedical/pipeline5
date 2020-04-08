package com.hartwig.pipeline.resource;

import static com.hartwig.pipeline.resource.ResourceNames.AMBER_PON;
import static com.hartwig.pipeline.resource.ResourceNames.BEDS;
import static com.hartwig.pipeline.resource.ResourceNames.GC_PROFILE;
import static com.hartwig.pipeline.resource.ResourceNames.GRIDSS_REPEAT_MASKER_DB;
import static com.hartwig.pipeline.resource.ResourceNames.MAPPABILITY;
import static com.hartwig.pipeline.resource.ResourceNames.REFERENCE_GENOME;
import static com.hartwig.pipeline.resource.ResourceNames.SAGE;
import static com.hartwig.pipeline.resource.ResourceNames.SNPEFF;

import com.hartwig.pipeline.execution.vm.VmDirectories;

public class Hg38ResourceFiles implements ResourceFiles {
    public static final String HG38_DIRECTORY = "hg38";

    public RefGenomeVersion version() { return RefGenomeVersion.HG38; }

    public String versionDirectory() { return HG38_DIRECTORY; }

    private String formPath(String name, String file) {
        return String.format("%s/%s/%s/%s", VmDirectories.RESOURCES, name, versionDirectory(), file);
    }

    public String refGenomeFile() { return formPath(REFERENCE_GENOME, "Homo_sapiens_assembly38.fasta"); }

    public String gcProfileFile() { return formPath(GC_PROFILE, "GC_profile.hg38.1000bp.cnp"); }

    public String amberHeterozygousLoci() { return formPath(AMBER_PON, "GermlineHetPon.hg38.vcf.gz"); }

    public String gridssRepeatMaskerDb() { return formPath(GRIDSS_REPEAT_MASKER_DB,"hg38.fa.out"); }
    public String gridssBlacklistBed() { return formPath(GRIDSS_REPEAT_MASKER_DB,"ENCFF001TDO.hg38.bed"); }

    public String snpEffDb() { return formPath(SNPEFF,"snpEff_v4_3_GRCh38.86.zip"); }
    public String snpEffVersion() { return "GRCh38.86"; }

    public String sageKnownHotspots() { return formPath(SAGE, "KnownHotspots.hg38.vcf.gz"); }

    public String sageActionableCodingPanel() { return formPath(SAGE, "ActionableCodingPanel.hg38.bed.gz"); }

    public String out150Mappability() { return formPath(MAPPABILITY, "out_150_hg38.mappability.bed.gz"); }

    public String sageGermlinePon() { return formPath(SAGE, "SageGermlinePon.hg38.vcf.gz"); }

    public String giabHighConfidenceBed() { return formPath(BEDS, "HG001_GRCh38_GIAB_highconf_CG-IllFB-IllGATKHC-Ion-10X-SOLID_CHROM1-X_v.3.3.2_highconf_nosomaticdel_noCENorHET7.bed.gz");  }
}
