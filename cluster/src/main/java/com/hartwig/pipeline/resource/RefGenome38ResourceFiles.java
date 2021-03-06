package com.hartwig.pipeline.resource;

import static com.hartwig.pipeline.resource.ResourceNames.AMBER;
import static com.hartwig.pipeline.resource.ResourceNames.COBALT;
import static com.hartwig.pipeline.resource.ResourceNames.ENSEMBL_DATA_CACHE;
import static com.hartwig.pipeline.resource.ResourceNames.FUSIONS;
import static com.hartwig.pipeline.resource.ResourceNames.GC_PROFILES;
import static com.hartwig.pipeline.resource.ResourceNames.GENE_PANEL;
import static com.hartwig.pipeline.resource.ResourceNames.GIAB_HIGH_CONF;
import static com.hartwig.pipeline.resource.ResourceNames.GRIDSS_PON;
import static com.hartwig.pipeline.resource.ResourceNames.GRIDSS_REPEAT_MASKER_DB;
import static com.hartwig.pipeline.resource.ResourceNames.LINX;
import static com.hartwig.pipeline.resource.ResourceNames.MAPPABILITY;
import static com.hartwig.pipeline.resource.ResourceNames.REFERENCE_GENOME;
import static com.hartwig.pipeline.resource.ResourceNames.SAGE;
import static com.hartwig.pipeline.resource.ResourceNames.SNPEFF;

public class RefGenome38ResourceFiles implements ResourceFiles {

    public RefGenomeVersion version() {
        return RefGenomeVersion.V38;
    }

    @Override
    public String versionDirectory() {
        return version().resources();
    }

    @Override
    public String refGenomeFile() {
        return formPath(REFERENCE_GENOME, "GCA_000001405.15_GRCh38_no_alt_analysis_set.fna");
    }

    @Override
    public String gcProfileFile() {
        return formPath(GC_PROFILES, "GC_profile.1000bp.38.cnp");
    }

    @Override
    public String diploidRegionsBed() {
        return formPath(COBALT, "DiploidRegions.38.bed.gz");
    }

    @Override
    public String amberHeterozygousLoci() {
        return formPath(AMBER, "GermlineHetPon.38.vcf.gz");
    }

    @Override
    public String amberSnpcheck() {
        return formPath(AMBER, "Amber.snpcheck.38.vcf");
    }

    @Override
    public String snpEffDb() {
        return formPath(SNPEFF, "snpEff_v4_3_GRCh38.86.zip");
    }

    @Override
    public String snpEffVersion() {
        return "GRCh38.86";
    }

    @Override
    public String sageSomaticHotspots() {
        return formPath(SAGE, "KnownHotspots.somatic.38.vcf.gz");
    }

    @Override
    public String sageSomaticCodingPanel() {
        return formPath(SAGE, "ActionableCodingPanel.somatic.38.bed.gz");
    }

    @Override
    public String sageGermlineHotspots() {
        return formPath(SAGE, "KnownHotspots.germline.38.vcf.gz");
    }

    @Override
    public String sageGermlineCodingPanel() {
        return formPath(SAGE, "ActionableCodingPanel.germline.38.bed.gz");
    }

    @Override
    public String sageGermlineCoveragePanel() {
        return formPath(SAGE, "CoverageCodingPanel.germline.38.bed.gz");
    }

    @Override
    public String sageGermlineSlicePanel() {
        return formPath(SAGE, "SlicePanel.germline.38.bed.gz");
    }

    @Override
    public String sageGermlineBlacklistVcf() {
        return formPath(SAGE, "KnownBlacklist.germline.38.vcf.gz");
    }

    @Override
    public String sageGermlineBlacklistBed() {
        return formPath(SAGE, "KnownBlacklist.germline.38.bed.gz");
    }

    @Override
    public String clinvarVcf() {
        return formPath(SAGE, "clinvar.38.vcf.gz");
    }

    @Override
    public String out150Mappability() {
        return formPath(MAPPABILITY, "out_150.mappability.38.bed.gz");
    }

    @Override
    public String sageGermlinePon() {
        return formPath(SAGE, "SageGermlinePon.98x.38.vcf.gz");
    }

    @Override
    public String giabHighConfidenceBed() {
        return formPath(GIAB_HIGH_CONF,
                "HG001_GRCh38_GIAB_highconf_CG-IllFB-IllGATKHC-Ion-10X-SOLID_CHROM1-X_v.3.3.2_highconf_nosomaticdel_noCENorHET7.bed.gz");
    }

    @Override
    public String gridssRepeatMaskerDb() {
        return formPath(GRIDSS_REPEAT_MASKER_DB, "38.fa.out");
    }

    @Override
    public String gridssBlacklistBed() {
        return formPath(GRIDSS_REPEAT_MASKER_DB, "ENCFF001TDO.38.bed");
    }

    @Override
    public String gridssBreakendPon() {
        return formPath(GRIDSS_PON, "gridss_pon_single_breakend.38.bed");
    }

    @Override
    public String gridssBreakpointPon() {
        return formPath(GRIDSS_PON, "gridss_pon_breakpoint.38.bedpe");
    }

    @Override
    public String fragileSites() {
        return formPath(LINX, "fragile_sites_hmf.38.csv");
    }

    @Override
    public String lineElements() {
        return formPath(LINX, "line_elements.38.csv");
    }

    @Override
    public String originsOfReplication() {
        return formPath(LINX, "heli_rep_origins_empty.bed");
    } // currently unsupported in 38

    @Override
    public String ensemblDataCache() {
        return formPath(ENSEMBL_DATA_CACHE, "");
    }

    @Override
    public String knownFusionData() {
        return formPath(FUSIONS, "known_fusion_data.38.csv");
    }

    @Override
    public String knownFusionPairBedpe() {
        return formPath(FUSIONS, "known_fusions.38.bedpe");
    }

    @Override
    public String genotypeSnpsDB() {
        throw new UnsupportedOperationException("[Genotype SNPs DB] does not yet have a valid 38 version.");
    }

    @Override
    public String driverGenePanel() {
        return formPath(GENE_PANEL, "DriverGenePanel.38.tsv");
    }

    @Override
    public String actionabilityDir() {
        throw new UnsupportedOperationException("Running PROTECT is not yet supported on 38 as we do not have the SERVE input.");
    }

    @Override
    public String peachFilterBed() {
        throw new UnsupportedOperationException("[PEACH filter BED] does not yet have a valid 38 version.");
    }
}
