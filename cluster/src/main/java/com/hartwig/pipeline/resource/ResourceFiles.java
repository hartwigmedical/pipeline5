package com.hartwig.pipeline.resource;

import com.hartwig.computeengine.execution.vm.VmDirectories;

import static com.hartwig.pipeline.resource.ResourceNames.*;

public interface ResourceFiles {

    static String of(final String name, final String file) {
        return String.format("%s/%s/%s", VmDirectories.RESOURCES, name, file);
    }

    static String of(final String name) {
        return of(name, "");
    }

    RefGenomeVersion version();

    String versionDirectory();

    String refGenomeFile();

    String gcProfileFile();

    String diploidRegionsBed();

    String amberHeterozygousLoci();

    String sageSomaticHotspots();

    String sagePanelBed();

    String sageGermlineHotspots();

    String sageGeneCoverageBed();

    String germlineBlacklistVcf();

    String germlineBlacklistBed();

    String clinvarVcf();

    String mappabilityBed();

    String germlinePon();

    String gnomadPonCache();

    String giabHighConfidenceBed();

    default String gridssPropertiesFile() {
        return of(GRIDSS, "gridss.properties");
    }

    String repeatMaskerDb();

    default String gridssVirusRefGenomeFile() {
        return of(VIRUS_REFERENCE_GENOME, "human_virus.fa");
    }

    String gridssBlacklistBed();

    String svPrepBlacklistBed();

    String sglBreakendPon();

    String svBreakpointPon();

    String ensemblDataCache();

    String knownFusionData();

    String knownFusionPairBedpe();

    String genotypeSnpsDB();

    String driverGenePanel();

    String hlaRegionBed();

    String purpleCohortGermlineDeletions();

    String targetRegionsPonArtefacts();

    String targetRegionsBed();

    String targetRegionsNormalisation();

    String targetRegionsRatios();

    String targetRegionsMsiIndels();

    default String cuppaRefData() {
        return of(CUPPA);
    }

    default String doidJson() {
        return of(DISEASE_ONTOLOGY, "doid.json");
    }

    default String snvSignatures() {
        return of(SIGS, "snv_cosmic_signatures.csv");
    }

    default String virusInterpreterTaxonomyDb() {
        return of(VIRUS_INTERPRETER, "taxonomy_db.tsv");
    }

    default String virusReportingDb() {
        return of(VIRUS_INTERPRETER, "virus_reporting_db.tsv");
    }

    default String orangeCohortMapping() {
        return of(ORANGE, "cohort_mapping.tsv");
    }

    default String orangeCohortPercentiles() {
        return of(ORANGE, "cohort_percentiles.tsv");
    }

    default String lilacResources() {
        return of(LILAC);
    }

    default String peachFilterBed() {
        return of(PEACH, "peach.json");
    }

    default String formPath(final String name, final String file) {
        return String.format("%s/%s/%s/%s", VmDirectories.RESOURCES, name, versionDirectory(), file);
    }
}
