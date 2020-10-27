package com.hartwig.pipeline.reruns;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;

import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.jackson.ObjectMappers;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.sbpapi.ImmutableSbpFileMetadata;
import com.hartwig.pipeline.sbpapi.SbpFileMetadata;
import com.hartwig.pipeline.sbpapi.SbpRestApi;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Before;
import org.junit.Test;

public class ApiPersistedDatasetTest {

    public static final ImmutableSbpFileMetadata FILE =
            SbpFileMetadata.builder().directory("dir").filename("file").filesize(1).hash("hash").run_id(1).build();
    private static final SingleSampleRunMetadata METADATA = TestInputs.referenceRunMetadata();
    private ApiPersistedDataset victim;

    @Before
    public void setUp() throws Exception {
        final SbpRestApi api = mock(SbpRestApi.class);
        when(api.getFileByBarcodeAndType(TestInputs.ID, METADATA.barcode(), "germline_variants")).thenReturn("[]");
        when(api.getFileByBarcodeAndType(TestInputs.ID, METADATA.barcode(), "aligned_reads")).thenReturn(String.format("[%s]",
                ObjectMappers.get().writeValueAsString(FILE)));
        victim = new ApiPersistedDataset(api, ObjectMappers.get());
    }

    @Test
    public void returnsEmptyPathWhenDataTypeNotFound() {
        assertThat(victim.file(METADATA, DataType.GERMLINE_VARIANTS)).isEmpty();
    }

    @Test
    public void returnsFileForDatatype() {
        Optional<String> maybePath = victim.file(METADATA, DataType.ALIGNED_READS);
        assertThat(maybePath).isPresent().hasValue("set/dir/file");
    }

    @Test
    public void returnsDirectoryForDatatype() {
        Optional<String> maybePath = victim.directory(METADATA, DataType.ALIGNED_READS);
        assertThat(maybePath).isPresent().hasValue("set/dir");
    }
}