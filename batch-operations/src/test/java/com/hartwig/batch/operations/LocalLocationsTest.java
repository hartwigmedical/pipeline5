package com.hartwig.batch.operations;

import static org.junit.Assert.assertEquals;

import java.util.List;

import com.hartwig.batch.api.LocalLocations;
import com.hartwig.batch.api.RemoteLocations;
import com.hartwig.pipeline.execution.vm.BashCommand;

import org.junit.Test;

public class LocalLocationsTest {
    private final static String PREFIX =
            "gsutil -o 'GSUtil:parallel_thread_count=1' -o GSUtil:sliced_object_download_max_components=$(nproc) -qm cp -r -n";
    private final RemoteLocations remoteLocations = new RemoteLocationsTestImpl();

    @Test
    public void testDownloadReferenceAlignmentIndex() {
        LocalLocations local = new LocalLocations(remoteLocations);
        String alignment = local.getReferenceAlignment();
        List<BashCommand> commands = local.generateDownloadCommands();

        assertEquals(commands.get(0).asBash(), PREFIX + " gs://alignment/171006_COLO829/COLO929v003R.cram /data/input/COLO929v003R.cram");
        assertEquals(commands.get(1).asBash(), PREFIX + " gs://alignment/171006_COLO829/COLO929v003R.crai /data/input/COLO929v003R.crai");
    }

    @Test
    public void testDownloadTumorAlignmentIndex() {
        LocalLocations local = new LocalLocations(remoteLocations);
        String alignment = local.getTumorAlignment();
        List<BashCommand> commands = local.generateDownloadCommands();

        assertEquals(commands.get(0).asBash(), "gsutil -o 'GSUtil:parallel_thread_count=1' -o GSUtil:sliced_object_download_max_components=$(nproc) -qm cp -r -n gs://alignment/171006_COLO829/COLO929v003T.cram /data/input/COLO929v003T.cram");
        assertEquals(commands.get(1).asBash(), "gsutil -o 'GSUtil:parallel_thread_count=1' -o GSUtil:sliced_object_download_max_components=$(nproc) -qm cp -r -n gs://alignment/171006_COLO829/COLO929v003T.crai /data/input/COLO929v003T.crai");
    }

    @Test(expected = IllegalStateException.class)
    public void testSelectFileAfterDownload() {
        LocalLocations local = new LocalLocations(remoteLocations);
        List<BashCommand> commands = local.generateDownloadCommands();
        String alignment = local.getTumorAlignment();
    }

}
