package com.hartwig.pipeline.execution;

import com.hartwig.computeengine.execution.vm.VmDirectories;
import com.hartwig.computeengine.storage.GoogleStorageLocation;
import org.immutables.value.Value;

import java.io.File;

@Value.Immutable
public interface OutputFile {

    @Value.Parameter
    String fileName();

    default String path() {
        return VmDirectories.OUTPUT + "/" + fileName();
    }

    default OutputFile index(final String suffix) {
        return ImmutableOutputFile.builder().from(this).fileName(fileName() + suffix).build();
    }

    static OutputFile of(final String sample, final String subStageName, final String type) {
        return ImmutableOutputFile.of(String.format("%s.%s.%s", sample, subStageName, type));
    }

    static OutputFile of(final String sample, final String type) {
        return ImmutableOutputFile.of(String.format("%s.%s", sample, type));
    }

    static OutputFile empty() {
        return ImmutableOutputFile.of("not.a.file");
    }

    default String copyToRemoteLocation(final GoogleStorageLocation remoteLocation) {
        String remoteDestination = remoteLocation.bucket() + File.separator + remoteLocation.path();
        if (remoteLocation.isDirectory()) {
            remoteDestination = remoteDestination + File.separator + fileName();
        }
        return String.format(
                "gsutil -o 'GSUtil:parallel_thread_count=1' -o \"GSUtil:sliced_object_download_max_components=$(nproc)\" -q cp %s gs://%s",
                path(),
                remoteDestination);
    }
}
