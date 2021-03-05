package com.hartwig.batch.operations;

import static org.junit.Assert.assertEquals;

import java.util.List;

import com.hartwig.batch.api.RemoteLocations;
import com.hartwig.pipeline.execution.vm.BashCommand;

import org.junit.Test;

public class PurpleRerunTest {
    final PurpleRerun victim = new PurpleRerun();

    @Test
    public void testCommands() {
        RemoteLocations locations = new RemoteLocationsTestImpl();
        List<BashCommand> commands = victim.bashCommands(locations);

        assertEquals(commands.get(0).asBash(), "mkdir -p /data/input/amber");
        assertEquals(commands.get(1).asBash(), "gsutil -qm cp -r -n gs://amber/171006_COLO829/* /data/input/amber/");
        assertEquals(commands.get(2).asBash(), "mkdir -p /data/input/cobalt");
        assertEquals(commands.get(3).asBash(), "gsutil -qm cp -r -n gs://cobalt/171006_COLO829/* /data/input/cobalt/");
        assertEquals(commands.get(4).asBash(), "gsutil -qm cp -r -n gs://sage/171006_COLO829/COLO929v003T.sage.somatic.vcf.gz /data/input/COLO929v003T.sage.somatic.vcf.gz");
        assertEquals(commands.get(5).asBash(), "gsutil -qm cp -r -n gs://sage/171006_COLO829/COLO929v003T.sage.germline.vcf.gz /data/input/COLO929v003T.sage.germline.vcf.gz");
        assertEquals(commands.get(6).asBash(), "gsutil -qm cp -r -n gs://gripss/171006_COLO829/COLO929v003T.gripss.somatic.filtered.vcf.gz /data/input/COLO929v003T.gripss.somatic.filtered.vcf.gz");
        assertEquals(commands.get(7).asBash(), "gsutil -qm cp -r -n gs://gripss/171006_COLO829/COLO929v003T.gripss.somatic.vcf.gz /data/input/COLO929v003T.gripss.somatic.vcf.gz");
        assertEquals(commands.get(8).asBash(), "gsutil -qm cp -r -n gs://gripss/171006_COLO829/COLO929v003T.gripss.somatic.vcf.gz.tbi /data/input/COLO929v003T.gripss.somatic.vcf.gz.tbi");
        assertEquals(commands.get(9).asBash().substring(0, 36), "java -Xmx12G -jar /opt/tools/purple/");
    }

}
