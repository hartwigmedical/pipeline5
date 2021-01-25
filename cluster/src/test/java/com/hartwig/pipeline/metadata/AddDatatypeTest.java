package com.hartwig.pipeline.metadata;

import static java.lang.String.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.sbpapi.AddFileApiResponse;
import com.hartwig.pipeline.sbpapi.SbpRestApi;

import org.junit.Test;

public class AddDatatypeTest {
    @Test
    public void shouldPatchFileWithDataTypeAndLinkFileToSample() {
        int id = 100;
        SbpRestApi api = mock(SbpRestApi.class);
        AddFileApiResponse file = mock(AddFileApiResponse.class);
        when(file.id()).thenReturn(id);
        AddDatatype victim = new AddDatatype(DataType.ALIGNED_READS, Folder.root(), format("%s/%s", "namespace", "filename"), "barcode");
        victim.apply(api, file);
        verify(api).patchFile(id, "datatype", DataType.ALIGNED_READS.toString().toLowerCase());
        verify(api).linkFileToSample(id, "barcode");
    }

    @Test
    public void shouldIncludeFolderInPathWhenItIsAFile() {
        SingleSampleRunMetadata metadata = mock(SingleSampleRunMetadata.class);
        when(metadata.sampleName()).thenReturn("sample_name");
        AddDatatype victim =
                new AddDatatype(DataType.ALIGNED_READS, Folder.from(metadata), format("%s/%s", "namespace", "filename"), "barcode");
        assertThat(victim.path()).isEqualTo("sample_name/namespace/filename");
    }

    @Test
    public void shouldCreateDirectory() {
        AddDatatype victim = new AddDatatype(DataType.ALIGNED_READS, Folder.root(), "namespace", "barcode");
        assertThat(victim.path()).isEqualTo("namespace");
    }
}
