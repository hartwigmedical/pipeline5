package com.hartwig.pipeline.resource;

public class OverriddenReferenceGenome implements ResourceFiles {

    private final ResourceFiles decorated;
    private final String referenceGenomeFile;

    public OverriddenReferenceGenome(final ResourceFiles decorated, final String referenceGenomeUrl) {
        this.decorated = decorated;
        this.referenceGenomeFile =
                OverrideReferenceGenomeCommand.RESOURCES_OVERRIDE + referenceGenomeUrl.substring(referenceGenomeUrl.lastIndexOf("/"));
    }

    @Override
    public String refGenomeFile() {
        return referenceGenomeFile;
    }

    @Override
    public RefGenomeVersion version() {
        return decorated.version();
    }

    @Override
    public String versionDirectory() {
        return decorated.versionDirectory();
    }

    @Override
    public String gcProfileFile() {
        return decorated.gcProfileFile();
    }

    @Override
    public String diploidRegionsBed() {
        return decorated.diploidRegionsBed();
    }

    @Override
    public String amberHeterozygousLoci() {
        return decorated.amberHeterozygousLoci();
    }

    @Override
    public String amberSnpcheck() {
        return decorated.amberSnpcheck();
    }

    @Override
    public String gridssRepeatMaskerDb() {
        return decorated.gridssRepeatMaskerDb();
    }

    @Override
    public String gridssBlacklistBed() {
        return decorated.gridssBlacklistBed();
    }

    @Override
    public String gridssBreakendPon() {
        return decorated.gridssBreakendPon();
    }

    @Override
    public String gridssBreakpointPon() {
        return decorated.gridssBreakpointPon();
    }

    @Override
    public String sageSomaticHotspots() {
        return decorated.sageSomaticHotspots();
    }

    @Override
    public String sageSomaticCodingPanel() {
        return decorated.sageSomaticCodingPanel();
    }

    @Override
    public String sageGermlineHotspots() {
        return decorated.sageGermlineHotspots();
    }

    @Override
    public String sageGermlineCodingPanel() {
        return decorated.sageGermlineCodingPanel();
    }

    @Override
    public String sageGermlineCoveragePanel() {
        return decorated.sageGermlineCoveragePanel();
    }

    @Override
    public String sageGermlineSlicePanel() {
        return decorated.sageGermlineSlicePanel();
    }

    @Override
    public String sageGermlineBlacklistVcf() {
        return decorated.sageGermlineBlacklistVcf();
    }

    @Override
    public String sageGermlineBlacklistBed() {
        return decorated.sageGermlineBlacklistBed();
    }

    @Override
    public String clinvarVcf() {
        return decorated.clinvarVcf();
    }

    @Override
    public String out150Mappability() {
        return decorated.out150Mappability();
    }

    @Override
    public String sageGermlinePon() {
        return decorated.sageGermlinePon();
    }

    @Override
    public String giabHighConfidenceBed() {
        return decorated.giabHighConfidenceBed();
    }

    @Override
    public String knownFusionPairBedpe() {
        return decorated.knownFusionPairBedpe();
    }

    @Override
    public String ensemblDataCache() {
        return decorated.ensemblDataCache();
    }

    @Override
    public String fragileSites() {
        return decorated.fragileSites();
    }

    @Override
    public String lineElements() {
        return decorated.lineElements();
    }

    @Override
    public String knownFusionData() {
        return decorated.knownFusionData();
    }

    @Override
    public String genotypeSnpsDB() {
        return decorated.genotypeSnpsDB();
    }

    @Override
    public String driverGenePanel() {
        return decorated.driverGenePanel();
    }

    @Override
    public String actionabilityDir() {
        return decorated.actionabilityDir();
    }

    @Override
    public String peachFilterBed() {
        return decorated.peachFilterBed();
    }
}
