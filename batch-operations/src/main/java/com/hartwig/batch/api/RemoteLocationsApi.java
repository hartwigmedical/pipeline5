package com.hartwig.batch.api;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Function;

import com.hartwig.api.DatasetApi;
import com.hartwig.api.HmfApi;
import com.hartwig.api.model.Dataset;
import com.hartwig.api.model.DatasetFile;
import com.hartwig.batch.input.InputFileDescriptor;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.ImmutableGoogleStorageLocation;

public class RemoteLocationsApi implements RemoteLocations {

    private final String tumor;
    private final String reference;
    private final String billedProject;
    private final Dataset dataset;

    public RemoteLocationsApi(final InputFileDescriptor biopsy) {
        this(biopsy.billedProject(), biopsy.inputValue());
    }

    public RemoteLocationsApi(final String billedProject, final String biopsy) {
        this.billedProject = billedProject;
        this.tumor = biopsy;
        this.reference = tumor.substring(0, tumor.lastIndexOf("T")) + "R";

        DatasetApi datasetApi = HmfApi.create(HmfApi.PRODUCTION).datasets();
        dataset = datasetApi.get(tumor, null, null);
    }

    @Override
    public String getTumor() {
        return tumor;
    }

    @Override
    public String getReference() {
        return reference;
    }

    @Override
    public GoogleStorageLocation getAmber() {
        return getLocation(tumor, Dataset::getAmber).asDirectory();
    }

    @Override
    public GoogleStorageLocation getCobalt() {
        return getLocation(tumor, Dataset::getCobalt).asDirectory();
    }

    @Override
    public GoogleStorageLocation getStructuralVariantsGridss() {
        return getLocation(tumor, Dataset::getStructuralVariantsGridss);
    }

    @Override
    public GoogleStorageLocation getStructuralVariantsGripss() {
        return getLocation(tumor, Dataset::getStructuralVariantsGripss);
    }

    @Override
    public GoogleStorageLocation getStructuralVariantsGripssRecovery() {
        return getLocation(tumor, Dataset::getStructuralVariantsGripssRecovery);
    }

    @Override
    public GoogleStorageLocation getStructuralVariantsGripssRecoveryIndex() {
        return index(getLocation(tumor, Dataset::getStructuralVariantsGripssRecovery));
    }

    @Override
    public GoogleStorageLocation getSomaticVariantsSage() {
        return getLocation(tumor, Dataset::getSomaticVariantsSage);
    }

    @Override
    public GoogleStorageLocation getGermlineVariantsSage() {
        return getLocation(tumor, Dataset::getGermlineVariantsSage);
    }

    @Override
    public GoogleStorageLocation getReferenceAlignment() {
        return getLocation(reference, Dataset::getAlignedReads);
    }

    @Override
    public GoogleStorageLocation getReferenceAlignmentIndex() {
        return getLocation(reference, Dataset::getAlignedReadsIndex);
    }

    @Override
    public GoogleStorageLocation getTumorAlignment() {
        return getLocation(tumor, Dataset::getAlignedReads);
    }

    @Override
    public GoogleStorageLocation getTumorAlignmentIndex() {
        return getLocation(tumor, Dataset::getAlignedReadsIndex);
    }

    private GoogleStorageLocation getLocation(String sample, Function<Dataset, Map<String, DatasetFile>> extractor) {
        String path = Optional.ofNullable(extractor.apply(dataset))
                .flatMap(x -> Optional.ofNullable(x.get(sample)))
                .flatMap(x -> Optional.ofNullable(x.getPath()))
                .orElseThrow(() -> new NoSuchElementException("Unable to find data for biopsy: " + tumor + ", sample: " + sample));
        return GoogleStorageLocation.from(path, billedProject);
    }

    private static GoogleStorageLocation index(GoogleStorageLocation victim) {
        if (victim.isDirectory()) {
            throw new IllegalArgumentException();
        }

        if (victim.path().endsWith(".bam")) {
            return ImmutableGoogleStorageLocation.builder().from(victim).path(victim.path() + ".bai").build();
        }

        if (victim.path().endsWith(".cram")) {
            return ImmutableGoogleStorageLocation.builder().from(victim).path(victim.path() + ".crai").build();
        }

        if (victim.path().endsWith(".vcf.gz")) {
            return ImmutableGoogleStorageLocation.builder().from(victim).path(victim.path() + ".tbi").build();
        }

        throw new IllegalArgumentException("Unknown file format");
    }

}
