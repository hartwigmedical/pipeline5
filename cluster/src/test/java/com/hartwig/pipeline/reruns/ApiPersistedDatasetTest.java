package com.hartwig.pipeline.reruns;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pdl.PipelineInput;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.input.TestJson;
import com.hartwig.pipeline.jackson.ObjectMappers;
import com.hartwig.pipeline.storage.GoogleStorageLocation;

import org.junit.Before;
import org.junit.Test;

public class ApiPersistedDatasetTest {

    public static final String SAMPLE = "CPCT12345678R";
    private InputPersistedDataset victim;

    @Before
    public void setUp() throws Exception {
        victim = new InputPersistedDataset(ObjectMappers.get().readValue(TestJson.get("with_dataset"), PipelineInput.class), "project");
    }

    @Test
    public void returnsEmptyIfDatatypeNotPresent() {
        assertThat(victim.path(SAMPLE, DataType.GERMLINE_VARIANTS)).isEmpty();
    }

    @Test
    public void returnsEmptyIfSampleNotPresent() {
        assertThat(victim.path(SAMPLE, DataType.SOMATIC_VARIANTS_SAGE)).isEmpty();
    }

    @Test
    public void returnsLocationOfExistingDataset() {
        assertThat(victim.path(SAMPLE, DataType.ALIGNED_READS)).contains(GoogleStorageLocation.from(
                "gs://output-bucket/CPCT12345678R/aligner/CPCT12345678R.bam",
                "project"));
    }

}