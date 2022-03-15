package com.hartwig.pipeline.resource;

import static com.hartwig.pipeline.resource.ResourceNames.CUPPA;
import static com.hartwig.pipeline.resource.ResourceNames.DISEASE_ONTOLOGY;
import static com.hartwig.pipeline.resource.ResourceNames.GRIDSS_CONFIG;
import static com.hartwig.pipeline.resource.ResourceNames.LILAC;
import static com.hartwig.pipeline.resource.ResourceNames.MAPPABILITY;
import static com.hartwig.pipeline.resource.ResourceNames.ORANGE;
import static com.hartwig.pipeline.resource.ResourceNames.SIGS;
import static com.hartwig.pipeline.resource.ResourceNames.VIRUS_INTERPRETER;
import static com.hartwig.pipeline.resource.ResourceNames.VIRUS_REFERENCE_GENOME;
import static com.hartwig.pipeline.resource.ResourceNames.PURPLE;

import java.util.Optional;

import com.hartwig.pipeline.execution.vm.VmDirectories;

public interface ResourceFiles {

    static String of(String name, String file) {
        return String.format("%s/%s/%s", VmDirectories.RESOURCES, name, file);
    }

    static String of(String name) {
        return of(name, "");
    }

    RefGenomeVersion version();

    String versionDirectory();

    String refGenomeFile();

    String gcProfileFile();

    String diploidRegionsBed();

    String amberHeterozygousLoci();

    String amberSnpcheck();

    String sageSomaticHotspots();

    String sageSomaticCodingPanel();

    String sageGermlineHotspots();

    String sageGermlineCodingPanel();

    String sageGermlineCoveragePanel();

    String sageGermlineSlicePanel();

    String sageGermlineBlacklistVcf();

    String sageGermlineBlacklistBed();

    String clinvarVcf();

    String out150Mappability();

    default String mappabilityHDR() {
        return of(MAPPABILITY, "mappability.hdr");
    }

    String sageGermlinePon();

    String germlinePon();

    String somaticPonArtefacts();

    String gnomadPonCache();

    String giabHighConfidenceBed();

    default String gridssPropertiesFile() {
        return of(GRIDSS_CONFIG, "gridss.properties");
    }

    String gridssRepeatMaskerDb();

    default String gridssRepeatMaskerDbBed() {
        return gridssRepeatMaskerDb() + ".bed";
    }

    default String gridssVirusRefGenomeFile() {
        return of(VIRUS_REFERENCE_GENOME, "human_virus.fa");
    }

    String gridssBlacklistBed();

    String gridssBreakendPon();

    String gridssBreakpointPon();

    String fragileSites();

    String lineElements();

    String ensemblDataCache();

    String knownFusionData();

    String knownFusionPairBedpe();

    String genotypeSnpsDB();

    String driverGenePanel();

    String actionabilityDir();

    String hlaRegionBed();

    String peachFilterBed();

    String purpleCohortGermlineDeletions();

    default Optional<String> targetRegionsBed() {
        return Optional.empty();
    }

    default String cuppaRefData() {
        return of(CUPPA);
    }

    default String doidJson() {
        return of(DISEASE_ONTOLOGY, "201015_doid.json");
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

    default String formPath(String name, String file) {
        return String.format("%s/%s/%s/%s", VmDirectories.RESOURCES, name, versionDirectory(), file);
    }
}
