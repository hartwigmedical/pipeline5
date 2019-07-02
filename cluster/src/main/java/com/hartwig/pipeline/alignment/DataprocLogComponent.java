package com.hartwig.pipeline.alignment;

import static java.lang.String.format;

import java.util.HashMap;
import java.util.Map;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.io.RuntimeBucket;
import com.hartwig.pipeline.report.ReportComponent;

import org.jetbrains.annotations.NotNull;

public class DataprocLogComponent implements ReportComponent {

    static final String METADATA_PATH = "google-cloud-dataproc-metainfo";
    private static final String LOG_EXTENSION = "_run.log";
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

    private void copyDataprocLogs(final Storage storage, final Bucket reportBucket, final String setName, final String sampleName,
            final RuntimeBucket alignerBucket) {
        Iterable<Blob> list = storage.list(alignerBucket.runId(), Storage.BlobListOption.prefix(METADATA_PATH)).iterateAll();
        Map<String, String> logsByStep = new HashMap<>();
        for (Blob blob : list) {
            String[] splitPath = blob.getName().split("/");
            if (splitPath[splitPath.length - 1].equals("driveroutput.000000000")) {
                if (splitPath.length > 4) {
                    String[] runIdSplit = splitPath[3].split("-");
                    String stepName = runIdSplit[runIdSplit.length - 1];
                    logsByStep.put(stepName, blob.getName());
                }
            }
        }
        for (Map.Entry<String, String> step : logsByStep.entrySet()) {
             storage.copy(Storage.CopyRequest.of(alignerBucket.runId(),
                    step.getValue(),
                    BlobId.of(reportBucket.getName(),
                            format("%s/%s/%s/%s", setName, sampleName, Aligner.NAMESPACE, withLogExtension(step.getKey())))));
        }
    }

    @NotNull
    private String withLogExtension(final String step) {
        return step + LOG_EXTENSION;
    }
}
