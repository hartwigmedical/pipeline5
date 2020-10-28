package com.hartwig.pipeline.reruns;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.jackson.ObjectMappers;
import com.hartwig.pipeline.metadata.TestJson;
import com.hartwig.pipeline.sbpapi.SbpRestApi;
import com.hartwig.pipeline.storage.GoogleStorageLocation;

import org.junit.Before;
import org.junit.Test;

public class ApiPersistedDatasetTest {

    private static final String BIOPSY = "biopsy";
    public static final String SAMPLE = "CPCT12345678R";
    private ApiPersistedDataset victim;

    @Before
    public void setUp() throws Exception {
        final SbpRestApi restApi = mock(SbpRestApi.class);
        when(restApi.getDataset(BIOPSY)).thenReturn(TestJson.get("get_dataset"));
        victim = new ApiPersistedDataset(restApi, ObjectMappers.get(), BIOPSY, "project");
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
        assertThat(victim.path(SAMPLE, DataType.ALIGNED_READS)).contains(GoogleStorageLocation.of("output-bucket",
                "CPCT12345678R/aligner/CPCT12345678R.bam"));
    }

}