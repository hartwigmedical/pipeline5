package com.hartwig.bcl2fastq.samplesheet;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;

public class SampleSheetCsv {

    private static final String SAMPLE_SHEET_CSV = "/SampleSheet.csv";
    private final Bucket inputBucket;
    private final String flowcell;

    public SampleSheetCsv(final Bucket inputBucket, final String flowcell) {
        this.inputBucket = inputBucket;
        this.flowcell = flowcell;
    }

    public SampleSheet read() {
        Blob blob = inputBucket.get(flowcell + SAMPLE_SHEET_CSV);
        if (blob != null) {
            String entireFile = new String(blob.getContent());
            String experimentName = entireFile.substring(entireFile.indexOf("ExperimentName")).split(",")[1];
            return SampleSheet.builder().experimentName(experimentName).addAllSamples(samples(entireFile)).build();
        }
        throw new IllegalArgumentException(String.format("No [%s] found for flowcell [%s] in bucket [%s]. Check inputs to bcl2fastq.",
                SAMPLE_SHEET_CSV,
                flowcell,
                inputBucket.getName()));
    }

    private List<IlluminaSample> samples(String entireFile) {
        try {
            CsvSchema schema = CsvSchema.emptySchema().withHeader();
            CsvMapper mapper = new CsvMapper();
            mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            return mapper.readerFor(IlluminaSample.class).with(schema).<IlluminaSample>readValues(entireFile.substring(entireFile.indexOf(
                    "Sample_ID"))).readAll().stream().filter(s -> !s.barcode().isEmpty()).collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
