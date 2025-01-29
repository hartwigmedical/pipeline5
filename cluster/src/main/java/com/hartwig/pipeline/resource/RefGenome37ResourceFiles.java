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

public class RefGenome37ResourceFiles implements ResourceFiles {

    private static final String REF_GENOME_FASTA_37_FILE = "Homo_sapiens.GRCh37.GATK.illumina.fasta";

    public RefGenomeVersion version() {
        return RefGenomeVersion.V37;
    }
    public String versionDirectory() {
        return version().numeric();
    }

    // ref genome and mappability
    public String refGenomeFile() {
        return formPath(REFERENCE_GENOME, REF_GENOME_FASTA_37_FILE);
    }
    public String mappabilityBed() {
        return formPath(MAPPABILITY, "mappability_150.37.bed.gz");
    }
    public String unmapRegionsFile() { return formPath(MAPPABILITY, "unmap_regions.37.tsv"); }
    public String msiJitterSitesFile() { return formPath(MAPPABILITY, "msi_jitter_sites.37.tsv.gz"); }

    // drivers and other common files
    public String driverGenePanel() {
        return formPath(GENE_PANEL, "DriverGenePanel.37.tsv");
    }
    public String ensemblDataCache() {
        return formPath(ENSEMBL_DATA_CACHE, "");
    }

    // copy number
    public String gcProfileFile() {
        return formPath(GC_PROFILES, "GC_profile.1000bp.37.cnp");
    }
    public String diploidRegionsBed() {
        return formPath(COBALT, "DiploidRegions.37.bed.gz");
    }
    public String amberHeterozygousLoci() {
        return formPath(AMBER, "AmberGermlineSites.37.tsv.gz");
    }
    public String purpleCohortGermlineDeletions() {
        return formPath(PURPLE, "cohort_germline_del_freq.37.csv");
    }

    // variant calling
    public String sageSomaticHotspots() {
        return formPath(SAGE, "KnownHotspots.somatic.37.vcf.gz");
    }
    public String sageGermlineHotspots() {
        return formPath(SAGE, "KnownHotspots.germline.37.vcf.gz");
    }
    public String sagePanelBed() {
        return formPath(SAGE, "ActionableCodingPanel.37.bed.gz");
    }
    public String sageGeneCoverageBed() {
        return formPath(SAGE, "CoverageCodingPanel.37.bed.gz");
    }
    public String germlineBlacklistVcf() {
        return formPath(SAGE, "KnownBlacklist.germline.37.vcf.gz");
    }
    public String germlineBlacklistBed() {
        return formPath(SAGE, "KnownBlacklist.germline.37.bed");
    }
    public String clinvarVcf() {
        return formPath(SAGE, "clinvar.37.vcf.gz");
    }
    public String germlinePon() {
        return formPath(SAGE, "SageGermlinePon.1000x.37.tsv.gz");
    }
    public String gnomadPonCache() {
        return formPath(GNOMAD, "gnomad_variants_v37.csv.gz");
    }

    public String giabHighConfidenceBed() {
        return formPath(GIAB_HIGH_CONF, "NA12878_GIAB_highconf_IllFB-IllGATKHC-CG-Ion-Solid_ALLCHROM_v3.2.2_highconf.bed.gz");
    }

    // structural variants and virus
    public String repeatMaskerDb() {
        return formPath(SV, "repeat_mask_data.37.fa.gz");
    }
    public String svPrepBlacklistBed() {
        return formPath(SV, "sv_prep_blacklist.37.bed");
    }
    public String decoyGenome() { return formPath(SV, "hg38_decoys.fa.img"); }
    public String sglBreakendPon() {
        return formPath(SV, "sgl_pon.37.bed.gz");
    }
    public String svBreakpointPon() {
        return formPath(SV, "sv_pon.37.bedpe.gz");
    }
    public String knownFusionData() {
        return formPath(SV, "known_fusion_data.37.csv");
    }
    public String knownFusionPairBedpe() {
        return formPath(SV, "known_fusions.37.bedpe");
    }

    // immune
    public String hlaRegionBed() {
        return formPath(LILAC, "hla.37.bed");
    }

    // CUP
    public String cuppaClassifier() {
        return formPath(CUPPA, "cuppa_classifier.37.pickle.gz");
    }
    public String cuppaCvPredictions() {
        return formPath(CUPPA, "cuppa_cv_predictions.37.tsv.gz");
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
        return formPath(PEACH, "haplotypes.37.tsv");
    }
    public String peachHaplotypeFunctions() {
        return formPath(PEACH, "haplotype_functions.37.tsv");
    }
    public String peachDrugs() {
        return formPath(PEACH, "peach_drugs.37.tsv");
    }

    // targeted panel
    public String targetRegionsPonArtefacts() {
        throw targetRegionsNotSupported();
    }
    public String targetRegionsBed() {
        throw targetRegionsNotSupported();
    }
    public String targetRegionsNormalisation() {
        throw targetRegionsNotSupported();
    }
    public String targetRegionsRatios() {
        throw targetRegionsNotSupported();
    }
    public String targetRegionsMsiIndels() {
        throw targetRegionsNotSupported();
    }

    private static UnsupportedOperationException targetRegionsNotSupported() {
        return new UnsupportedOperationException("Target regions are not supported in HG37");
    }

}
