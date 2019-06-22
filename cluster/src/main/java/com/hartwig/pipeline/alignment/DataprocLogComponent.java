package com.hartwig.pipeline.alignment;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.io.RuntimeBucket;
import com.hartwig.pipeline.results.ReportComponent;

public class DataprocLogComponent implements ReportComponent {

    static final String METADATA_PATH = "google-cloud-dataproc-metainfo";
    private final Sample sample;
    private final RuntimeBucket runtimeBucket;

    DataprocLogComponent(final Sample sample, final RuntimeBucket runtimeBucket) {
        this.sample = sample;
        this.runtimeBucket = runtimeBucket;
    }

    @Override
    public void addToReport(final Storage storage, final Bucket reportBucket, final String setName) {
        copyDataprocLogs(storage, reportBucket, setName, sample.name(), runtimeBucket);
    }

    private void copyDataprocLogs(final Storage storage, final Bucket reportBucket, final String setName, final String referenceSampleName,
            final RuntimeBucket alignerBucket) {
        Iterable<Blob> list = storage.list(alignerBucket.runId(), Storage.BlobListOption.prefix(METADATA_PATH)).iterateAll();
        for (Blob blob : list) {
            String[] splitPath = blob.getName().split("/");
            if (splitPath[splitPath.length - 1].contains("driveroutput")) {
                if (splitPath.length > 4) {
                    String[] runIdSplit = splitPath[3].split("-");
                    String stepName = runIdSplit[runIdSplit.length - 1];
                    String logName = splitPath[splitPath.length - 1];
                    storage.copy(Storage.CopyRequest.of(alignerBucket.runId(),
                            blob.getName(),
                            BlobId.of(reportBucket.getName(),
                                    String.format("%s/%s/%s/%s/%s", setName, referenceSampleName, Aligner.NAMESPACE, stepName, logName))));
                }
            }
        }
    }
}
