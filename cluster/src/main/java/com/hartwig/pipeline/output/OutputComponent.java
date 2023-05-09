package com.hartwig.pipeline.output;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;

public interface OutputComponent {

    void addToOutput(final Storage storage, final Bucket outputBucket, final String setName);
}
