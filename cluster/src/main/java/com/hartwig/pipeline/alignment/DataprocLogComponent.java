package com.hartwig.pipeline.alignment;

import static java.lang.String.format;

import java.util.Collection;
import java.util.stream.Collectors;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.io.ResultsDirectory;
import com.hartwig.pipeline.io.RuntimeBucket;
import com.hartwig.pipeline.report.ReportComponent;

import org.jetbrains.annotations.NotNull;

public class DataprocLogComponent implements ReportComponent {

    static final String METADATA_PATH = "google-cloud-dataproc-metainfo";
    private static final String LOG_EXTENSION = "-run.log";
    private final Sample sample;
    private final RuntimeBucket runtimeBucket;
    private final ResultsDirectory resultsDirectory;

    DataprocLogComponent(final Sample sample, final RuntimeBucket runtimeBucket, final ResultsDirectory resultsDirectory) {
        this.sample = sample;
        this.runtimeBucket = runtimeBucket;
        this.resultsDirectory = resultsDirectory;
    }

    @Override
    public void addToReport(final Storage storage, final Bucket reportBucket, final String setName) {
        copyDataprocLogs(storage, reportBucket, setName, sample.name(), runtimeBucket);
    }

    private void copyDataprocLogs(final Storage storage, final Bucket reportBucket, final String setName, final String sampleName,
            final RuntimeBucket alignerBucket) {
        Iterable<Blob> list = storage.list(alignerBucket.runId(), Storage.BlobListOption.prefix(METADATA_PATH)).iterateAll();
        Multimap<String, String> logsByStep = ArrayListMultimap.create();
        for (Blob blob : list) {
            String[] splitPath = blob.getName().split("/");
            if (splitPath[splitPath.length - 1].contains("driveroutput")) {
                if (splitPath.length > 4) {
                    String[] runIdSplit = splitPath[3].split("-");
                    String stepName = runIdSplit[runIdSplit.length - 1];
                    logsByStep.put(stepName, blob.getName());
                }
            }
        }
        for (String step : logsByStep.keySet()) {
            Collection<String> logBlobs = logsByStep.get(step).stream().sorted().collect(Collectors.toList());
            String tempComposedLog = format("%s/%s", Aligner.NAMESPACE, resultsDirectory.path(withLogExtension(step)));
            storage.compose(Storage.ComposeRequest.of(logBlobs, BlobInfo.newBuilder(alignerBucket.runId(), tempComposedLog).build()));
            storage.copy(Storage.CopyRequest.of(alignerBucket.runId(),
                    tempComposedLog,
                    BlobId.of(reportBucket.getName(),
                            format("%s/%s/%s/working/%s", setName, sampleName, Aligner.NAMESPACE, withLogExtension(step)))));
        }
    }

    @NotNull
    private String withLogExtension(final String step) {
        return step + LOG_EXTENSION;
    }
}
