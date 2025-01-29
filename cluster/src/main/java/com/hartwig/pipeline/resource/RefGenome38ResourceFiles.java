package com.hartwig.pipeline.resource;

import static com.hartwig.pipeline.resource.ResourceNames.AMBER;
import static com.hartwig.pipeline.resource.ResourceNames.COBALT;
import static com.hartwig.pipeline.resource.ResourceNames.CUPPA;
import static com.hartwig.pipeline.resource.ResourceNames.ENSEMBL_DATA_CACHE;
import static com.hartwig.pipeline.resource.ResourceNames.GC_PROFILES;
import static com.hartwig.pipeline.resource.ResourceNames.GENE_PANEL;
import static com.hartwig.pipeline.resource.ResourceNames.GENOTYPE_SNPS;
import static com.hartwig.pipeline.resource.ResourceNames.GIAB_HIGH_CONF;
import static com.hartwig.pipeline.resource.ResourceNames.GNOMAD;
import static com.hartwig.pipeline.resource.ResourceNames.SV;
import static com.hartwig.pipeline.resource.ResourceNames.LILAC;
import static com.hartwig.pipeline.resource.ResourceNames.MAPPABILITY;
import static com.hartwig.pipeline.resource.ResourceNames.PEACH;
import static com.hartwig.pipeline.resource.ResourceNames.PURPLE;
import static com.hartwig.pipeline.resource.ResourceNames.REFERENCE_GENOME;
import static com.hartwig.pipeline.resource.ResourceNames.SAGE;

public class RefGenome38ResourceFiles implements ResourceFiles {

    public RefGenomeVersion version() {
        return RefGenomeVersion.V38;
    }
    public String versionDirectory() {
        return version().numeric();
    }

    // ref genome and mappability
    public String refGenomeFile() {
        return formPath(REFERENCE_GENOME, "Homo_sapiens_assembly38.alt.masked.fasta");
    }
    public String mappabilityBed() {
        return formPath(MAPPABILITY, "mappability_150.38.bed.gz");
    }
    public String unmapRegionsFile() { return formPath(MAPPABILITY, "unmap_regions.38.tsv"); }
    public String msiJitterSitesFile() { return formPath(MAPPABILITY, "msi_jitter_sites.38.tsv.gz"); }

    // drivers and other common files
    public String ensemblDataCache() {
        return formPath(ENSEMBL_DATA_CACHE, "");
    }
    public String driverGenePanel() {
        return formPath(GENE_PANEL, "DriverGenePanel.38.tsv");
    }

    // copy number
    public String gcProfileFile() {
        return formPath(GC_PROFILES, "GC_profile.1000bp.38.cnp");
    }
    public String diploidRegionsBed() {
        return formPath(COBALT, "DiploidRegions.38.bed.gz");
    }
    public String amberHeterozygousLoci() {
        return formPath(AMBER, "AmberGermlineSites.38.tsv.gz");
    }
    public String purpleCohortGermlineDeletions() {
        return formPath(PURPLE, "cohort_germline_del_freq.38.csv");
    }

    // variant calling
    public String sageSomaticHotspots() {
        return formPath(SAGE, "KnownHotspots.somatic.38.vcf.gz");
    }
    public String sageGermlineHotspots() {
        return formPath(SAGE, "KnownHotspots.germline.38.vcf.gz");
    }
    public String sagePanelBed() {
        return formPath(SAGE, "ActionableCodingPanel.38.bed.gz");
    }
    public String sageGeneCoverageBed() {
        return formPath(SAGE, "CoverageCodingPanel.38.bed.gz");
    }
    public String germlineBlacklistVcf() {
        return formPath(SAGE, "KnownBlacklist.germline.38.vcf.gz");
    }
    public String germlineBlacklistBed() {
        return formPath(SAGE, "KnownBlacklist.germline.38.bed");
    }
    public String clinvarVcf() {
        return formPath(SAGE, "clinvar.38.vcf.gz");
    }
    public String germlinePon() {
        return formPath(SAGE, "SageGermlinePon.98x.38.tsv.gz");
    }
    public String gnomadPonCache() {
        return formPath(GNOMAD, "");
    }

    public String giabHighConfidenceBed() {
        return formPath(GIAB_HIGH_CONF,
                "HG001_GRCh38_GIAB_highconf_CG-IllFB-IllGATKHC-Ion-10X-SOLID_CHROM1-X_v.3.3.2_highconf_nosomaticdel_noCENorHET7.bed.gz");
    }

    // structural variants and virus
    public String repeatMaskerDb() {
        return formPath(SV, "repeat_mask_data.38.fa.gz");
    }
    public String svPrepBlacklistBed() {
        return formPath(SV, "sv_prep_blacklist.38.bed");
    }
    public String decoyGenome() { return formPath(SV, ""); } // not required
    public String sglBreakendPon() {
        return formPath(SV, "sgl_pon.38.bed.gz");
    }
    public String svBreakpointPon() {
        return formPath(SV, "sv_pon.38.bedpe.gz");
    }
    public String knownFusionData() {
        return formPath(SV, "known_fusion_data.38.csv");
    }
    public String knownFusionPairBedpe() { return formPath(SV, "known_fusions.38.bedpe"); }

    // immune
    public String hlaRegionBed() {
        return formPath(LILAC, "hla.38.bed");
    }

    // misc other
    public String genotypeSnpsDB() {
        return formPath(GENOTYPE_SNPS, "26SNPtaq.vcf");
    }
    @Override
    public String genotypeMipSnpsDB() {
        return formPath(GENOTYPE_SNPS, "31SNPtaq.vcf");
    }
    public String peachHaplotypes() {
        return formPath(PEACH, "haplotypes.38.tsv");
    }
    public String peachHaplotypeFunctions() {
        return formPath(PEACH, "haplotype_functions.38.tsv");
    }
    public String peachDrugs() {
        return formPath(PEACH, "peach_drugs.38.tsv");
    }

    // targeted panel
    public String targetRegionsPonArtefacts() {
        return formPath(ResourceNames.TARGET_REGIONS, "target_regions_pon_artefacts.38.tsv.gz");
    }
    public String targetRegionsBed() {
        return formPath(ResourceNames.TARGET_REGIONS, "target_regions_definition.38.bed");
    }
    public String targetRegionsNormalisation() {
        return formPath(ResourceNames.TARGET_REGIONS, "target_regions_normalisation.38.tsv");
    }
    public String targetRegionsRatios() {
        return formPath(ResourceNames.TARGET_REGIONS, "target_regions_ratios.38.tsv");
    }
    public String targetRegionsMsiIndels() {
        return formPath(ResourceNames.TARGET_REGIONS, "target_regions_msi_indels.38.tsv");
    }

    // hg38 model for CUPPA does not yet exist
    public String cuppaClassifier() {
        return formPath(CUPPA, "cuppa_classifier.38.pickle.gz");
    }
    public String cuppaCvPredictions() {
        return formPath(CUPPA, "cuppa_cv_predictions.38.tsv.gz");
    }
}
