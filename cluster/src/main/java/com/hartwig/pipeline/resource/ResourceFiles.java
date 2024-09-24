package com.hartwig.pipeline.resource;

import static com.hartwig.pipeline.resource.ResourceNames.BLAST_DB;
import static com.hartwig.pipeline.resource.ResourceNames.CUPPA;
import static com.hartwig.pipeline.resource.ResourceNames.DISEASE_ONTOLOGY;
import static com.hartwig.pipeline.resource.ResourceNames.GRIDSS;
import static com.hartwig.pipeline.resource.ResourceNames.LILAC;
import static com.hartwig.pipeline.resource.ResourceNames.ORANGE;
import static com.hartwig.pipeline.resource.ResourceNames.PEACH;
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
    String unmapRegionsFile();

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

    String genotypeMipSnpsDB();

    String driverGenePanel();

    String hlaRegionBed();

    String purpleCohortGermlineDeletions();

    String targetRegionsPonArtefacts();
    String targetRegionsBed();
    String targetRegionsNormalisation();
    String targetRegionsRatios();
    String targetRegionsMsiIndels();

    String cuppaClassifier();
    String cuppaCvPredictions();

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

    default String blastDb() {
        return ResourceFiles.of(BLAST_DB);
    }

    default String formPath(final String name, final String file) {
        return String.format("%s/%s/%s/%s", VmDirectories.RESOURCES, name, versionDirectory(), file);
    }
}
