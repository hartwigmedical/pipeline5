package com.hartwig.pipeline.resource;

import static com.hartwig.pipeline.resource.ResourceNames.AMBER;
import static com.hartwig.pipeline.resource.ResourceNames.COBALT;
import static com.hartwig.pipeline.resource.ResourceNames.ENSEMBL_DATA_CACHE;
import static com.hartwig.pipeline.resource.ResourceNames.FUSIONS;
import static com.hartwig.pipeline.resource.ResourceNames.GC_PROFILES;
import static com.hartwig.pipeline.resource.ResourceNames.GENE_PANEL;
import static com.hartwig.pipeline.resource.ResourceNames.GENOTYPE_SNPS;
import static com.hartwig.pipeline.resource.ResourceNames.GIAB_HIGH_CONF;
import static com.hartwig.pipeline.resource.ResourceNames.GNOMAD;
import static com.hartwig.pipeline.resource.ResourceNames.GRIDSS_PON;
import static com.hartwig.pipeline.resource.ResourceNames.GRIDSS_REPEAT_MASKER_DB;
import static com.hartwig.pipeline.resource.ResourceNames.LILAC;
import static com.hartwig.pipeline.resource.ResourceNames.LINX;
import static com.hartwig.pipeline.resource.ResourceNames.MAPPABILITY;
import static com.hartwig.pipeline.resource.ResourceNames.PURPLE;
import static com.hartwig.pipeline.resource.ResourceNames.REFERENCE_GENOME;
import static com.hartwig.pipeline.resource.ResourceNames.SAGE;
import static com.hartwig.pipeline.resource.ResourceNames.SERVE;

public class RefGenome38ResourceFiles implements ResourceFiles {

    public RefGenomeVersion version() {
        return RefGenomeVersion.V38;
    }

    @Override
    public String versionDirectory() {
        return version().numeric();
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
    public String sageSomaticHotspots() {
        return formPath(SAGE, "KnownHotspots.somatic.38.vcf.gz");
    }

    @Override
    public String sageGermlineHotspots() {
        return formPath(SAGE, "KnownHotspots.germline.38.vcf.gz");
    }

    @Override
    public String sagePanelBed() {
        return formPath(SAGE, "ActionableCodingPanel.38.bed.gz");
    }

    @Override
    public String sageGeneCoverageBed() {
        return formPath(SAGE, "CoverageCodingPanel.38.bed.gz");
    }

    @Override
    public String germlineBlacklistVcf() {
        return formPath(SAGE, "KnownBlacklist.germline.38.vcf.gz");
    }

    @Override
    public String germlineBlacklistBed() {
        return formPath(SAGE, "KnownBlacklist.germline.38.bed");
    }

    @Override
    public String clinvarVcf() {
        return formPath(SAGE, "clinvar.38.vcf.gz");
    }

    @Override
    public String mappabilityBed() {
        return formPath(MAPPABILITY, "mappability_150.38.bed.gz");
    }

    @Override
    public String sageGermlinePon() {
        return formPath(SAGE, "SageGermlinePon.98x.38.vcf.gz");
    }

    @Override
    public String germlinePon() {
        return formPath(SAGE, "SageGermlinePon.98x.38.tsv.gz");
    }

    @Override
    public String somaticPonArtefacts() {
        return formPath(SAGE, "PanelArtefacts.38.tsv");
    }

    @Override
    public String gnomadPonCache() {
        return formPath(GNOMAD, "");
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
    public String svBreakendPon() {
        return formPath(GRIDSS_PON, "gridss_pon_single_breakend.38.bed.gz");
    }

    @Override
    public String svBreakpointPon() {
        return formPath(GRIDSS_PON, "gridss_pon_breakpoint.38.bedpe.gz");
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
        return formPath(GENOTYPE_SNPS, "26SNPtaq.vcf");
    }

    @Override
    public String driverGenePanel() {
        return formPath(GENE_PANEL, "DriverGenePanel.38.tsv");
    }

    @Override
    public String actionabilityDir() {
        return formPath(SERVE, "");
    }

    @Override
    public String hlaRegionBed() {
        return formPath(LILAC, "hla.38.bed");
    }

    @Override
    public String purpleCohortGermlineDeletions() {
        return formPath(PURPLE, "cohort_germline_del_freq.38.csv");
    }

    @Override
    public String targetRegionsBed() {
        return formPath(ResourceNames.TARGET_REGIONS, "target_regions_definition.38.bed");
    }

    @Override
    public String targetRegionsNormalisation() {
        return formPath(ResourceNames.TARGET_REGIONS, "target_regions_normalisation.38.tsv");
    }

    @Override
    public String targetRegionsRatios() {
        return formPath(ResourceNames.TARGET_REGIONS, "target_regions_ratios.38.csv");
    }

    @Override
    public String targetRegionsInterval() {
        return formPath(ResourceNames.TARGET_REGIONS, "target_regions_definition.38.interval_list");
    }

    @Override
    public String targetRegionsMsiIndels() {
        return formPath(ResourceNames.TARGET_REGIONS, "target_regions_msi_indels.38.tsv");
    }
}
