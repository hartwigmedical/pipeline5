package com.hartwig.pipeline.resource;

import static com.hartwig.pipeline.resource.ResourceNames.BLAST_DB;
import static com.hartwig.pipeline.resource.ResourceNames.DISEASE_ONTOLOGY;
import static com.hartwig.pipeline.resource.ResourceNames.LILAC;
import static com.hartwig.pipeline.resource.ResourceNames.ORANGE;
import static com.hartwig.pipeline.resource.ResourceNames.SIGS;
import static com.hartwig.pipeline.resource.ResourceNames.VIRUS_INTERPRETER;
import static com.hartwig.pipeline.resource.ResourceNames.VIRUS_REFERENCE_GENOME;

import com.hartwig.computeengine.execution.vm.VmDirectories;

public interface ResourceFiles {

    static String of(final String name, final String file) {
        return String.format("%s/%s/%s", VmDirectories.RESOURCES, name, file);
    }

    static String of(final String name) {
        return of(name, "");
    }

    RefGenomeVersion version();
        String versionDirectory();

    // ref genome and mappability
    String refGenomeFile();
    String mappabilityBed();
    String unmapRegionsFile();
    String msiJitterSitesFile();

    // drivers and other common files
    String ensemblDataCache();
    String driverGenePanel();

    // copy number
    String gcProfileFile();
    String diploidRegionsBed();
    String amberHeterozygousLoci();
    String purpleCohortGermlineDeletions();

    // variant calling
    String sageSomaticHotspots();
    String sagePanelBed();
    String sageGermlineHotspots();
    String sageGeneCoverageBed();
    String germlineBlacklistVcf();
    String germlineBlacklistBed();
    String clinvarVcf();
    String germlinePon();
    String gnomadPonCache();
    String giabHighConfidenceBed();
    String genotypeSnpsDB(); // will be decommission when Sage germline covers entire genome or exome
    String genotypeMipSnpsDB(); // comment about decommission for genotypeSnpsDB above also applies here

    // structural variants and virus
    String repeatMaskerDb();
    String svPrepBlacklistBed();
    String decoyGenome();
    String sglBreakendPon();
    String svBreakpointPon();
    String knownFusionData();
    String knownFusionPairBedpe();

    default String gridssVirusRefGenomeFile() {
        return of(VIRUS_REFERENCE_GENOME, "human_virus.fa");
    }

    // immune
    String hlaRegionBed();

    // targeted panel
    String targetRegionsPonArtefacts();
    String targetRegionsBed();
    String targetRegionsNormalisation();
    String targetRegionsRatios();
    String targetRegionsMsiIndels();

    // CUP
    String cuppaClassifier();
    String cuppaCvPredictions();

    // misc other
    String peachHaplotypes();
    String peachHaplotypeFunctions();
    String peachDrugs();

    default String doidJson() {
        return of(DISEASE_ONTOLOGY, "doid.json");
    }

    default String snvSignatures() { return of(SIGS, "snv_cosmic_signatures.csv"); }
    default String signaturesEtiology() { return of(SIGS, "signatures_etiology.tsv"); }

    default String virusInterpreterTaxonomyDb() {
        return of(VIRUS_INTERPRETER, "taxonomy_db.tsv");
    }
    default String virusReportingDb() {
        return of(VIRUS_INTERPRETER, "virus_reporting_db.tsv");
    }
    default String virusBlacklistingDb() { return of(VIRUS_INTERPRETER, "virus_blacklisting_db.tsv"); }

    default String orangeCohortMapping() {
        return of(ORANGE, "cohort_mapping.tsv");
    }

    default String orangeCohortPercentiles() {
        return of(ORANGE, "cohort_percentiles.tsv");
    }

    default String lilacResources() {
        return of(LILAC);
    }

    default String blastDb() {
        return ResourceFiles.of(BLAST_DB);
    }

    default String vChordModel() {
        return of(ResourceNames.V_CHORD, "vchord_model.targeted.pt");
    }

    default String formPath(final String name, final String file) {
        return String.format("%s/%s/%s/%s", VmDirectories.RESOURCES, name, versionDirectory(), file);
    }
}
