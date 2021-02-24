package com.hartwig.batch.api;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Function;

import com.hartwig.api.DatasetApi;
import com.hartwig.api.HmfApi;
import com.hartwig.api.model.Dataset;
import com.hartwig.api.model.DatasetFile;
import com.hartwig.batch.input.ImmutableInputFileDescriptor;
import com.hartwig.batch.input.InputFileDescriptor;

public class ApiInputFileDescriptorFactory {

    private final String tumor;
    private final String reference;
    private final InputFileDescriptor template;
    private final Dataset dataset;

    public ApiInputFileDescriptorFactory(final InputFileDescriptor biopsy) {
        this.template = biopsy;
        this.tumor = biopsy.inputValue();
        this.reference = tumor.substring(0, tumor.lastIndexOf("T")) + "R";

        // THIS IS TEMPORARY (Should not need the 1's at the end)
        String certLocation = System.getProperty("javax.net.ssl.keyStore1");
        String certPassword = System.getProperty("javax.net.ssl.keyStorePassword1");

        DatasetApi datasetApi = HmfApi.createSSL(HmfApi.PRODUCTION, certLocation, certPassword).datasets();
        dataset = datasetApi.get(tumor);
    }

    public String getTumor() {
        return tumor;
    }

    public String getReference() {
        return reference;
    }

    public InputFileDescriptor getGridssUnfilteredOutput() {
        return get(tumor, Dataset::getStructuralVariantsGridss);
    }

    public InputFileDescriptor getReferenceAlignment() {
        return get(reference, Dataset::getAlignedReads);
    }

    public InputFileDescriptor getReferenceAlignmentIndex() {
        return get(reference, Dataset::getAlignedReadsIndex);
    }

    public InputFileDescriptor getTumorAlignment() {
        return get(tumor, Dataset::getAlignedReads);
    }

    public InputFileDescriptor getTumorAlignmentIndex() {
        return get(tumor, Dataset::getAlignedReadsIndex);
    }

    private InputFileDescriptor get(String sample, Function<Dataset, Map<String, DatasetFile>> extractor) {
        String path = Optional.ofNullable(extractor.apply(dataset))
                .flatMap(x -> Optional.ofNullable(x.get(sample)))
                .flatMap(x -> Optional.ofNullable(x.getPath()))
                .orElseThrow( () -> new NoSuchElementException("Unable to find data for biopsy: " + tumor + ", sample: " + sample));

        return ImmutableInputFileDescriptor.builder().from(template).inputValue(path).build();

    }

}
