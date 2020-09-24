package com.hartwig.pipeline.transfer;

import static com.hartwig.pipeline.testsupport.TestBlobs.blob;
import static com.hartwig.pipeline.testsupport.TestBlobs.pageOf;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Test;

public class OutputIteratorTest {

    @Test
    public void iteratedOverBlobsInBucket(){
        List<Blob> iterated = new ArrayList<>();
        Bucket sourceBucket = mock(Bucket.class);
        Blob first = blob("1");
        Blob second = blob("2");
        Page<Blob> page = pageOf(first, second);
        when(sourceBucket.list(Storage.BlobListOption.prefix("set/"))).thenReturn(page);
        OutputIterator victim = OutputIterator.from(iterated::add, sourceBucket);
        victim.iterate(TestInputs.defaultSomaticRunMetadata());
        assertThat(iterated).hasSize(2);
        assertThat(iterated.get(0).getName()).isEqualTo("1");
        assertThat(iterated.get(1).getName()).isEqualTo("2");
    }
}