package com.hartwig.pipeline.metadata;

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
        AddDatatype victim = new AddDatatype(DataType.ALIGNED_READS, "barcode", new ArchivePath(Folder.root(), "namespace", "filename"));
        victim.apply(api, file);
        verify(api).patchFile(id, "datatype", DataType.ALIGNED_READS.toString().toLowerCase());
        verify(api).linkFileToSample(id, "barcode");
    }
}
