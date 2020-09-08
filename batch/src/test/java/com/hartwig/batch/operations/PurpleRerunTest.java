package com.hartwig.batch.operations;

import static org.junit.Assert.assertEquals;

import java.util.List;

import com.hartwig.batch.input.ImmutableInputFileDescriptor;
import com.hartwig.batch.input.InputBundle;
import com.hartwig.batch.input.InputFileDescriptor;
import com.hartwig.pipeline.execution.vm.BashCommand;

import org.assertj.core.util.Lists;
import org.junit.Test;

public class PurpleRerunTest {
    final PurpleRerun victim = new PurpleRerun();

    @Test
    public void testCommands() {
        InputBundle inputBundle = createInput("171006_COLO829", "COLO929v003T", "COLO929v003R");
        List<BashCommand> commands = victim.bashCommands(inputBundle);

        assertEquals(commands.get(0).asBash(), "mkdir -p /data/input/amber");
        assertEquals(commands.get(1).asBash(), "mkdir -p /data/input/cobalt");
        assertEquals(commands.get(2).asBash(), "gsutil -qm cp -r -n gs://hmf-amber/171006_COLO829/* /data/input/amber/");
        assertEquals(commands.get(3).asBash(), "gsutil -qm cp -r -n gs://hmf-cobalt/171006_COLO829/* /data/input/cobalt/");
        assertEquals(commands.get(4).asBash(), "gsutil -qm cp -r -n gs://hmf-sage/171006_COLO829/COLO929v003T.sage.somatic.filtered.vcf.gz /data/input/COLO929v003T.sage.somatic.filtered.vcf.gz");
        assertEquals(commands.get(5).asBash(), "gsutil -qm cp -r -n gs://hmf-sage/171006_COLO829/COLO929v003T.sage.somatic.filtered.vcf.gz.tbi /data/input/COLO929v003T.sage.somatic.filtered.vcf.gz.tbi");
        assertEquals(commands.get(6).asBash(), "gsutil -qm cp -r -n gs://hmf-gripss/171006_COLO829/COLO929v003T.gridss.somatic.filtered.vcf.gz /data/input/COLO929v003T.gridss.somatic.filtered.vcf.gz");
        assertEquals(commands.get(7).asBash(), "gsutil -qm cp -r -n gs://hmf-gripss/171006_COLO829/COLO929v003T.gridss.somatic.filtered.vcf.gz.tbi /data/input/COLO929v003T.gridss.somatic.filtered.vcf.gz.tbi");
        assertEquals(commands.get(8).asBash(), "gsutil -qm cp -r -n gs://hmf-gripss/171006_COLO829/COLO929v003T.gridss.somatic.vcf.gz /data/input/COLO929v003T.gridss.somatic.vcf.gz");
        assertEquals(commands.get(9).asBash(), "gsutil -qm cp -r -n gs://hmf-gripss/171006_COLO829/COLO929v003T.gridss.somatic.vcf.gz.tbi /data/input/COLO929v003T.gridss.somatic.vcf.gz.tbi");
        assertEquals(commands.get(10).asBash().substring(0, 36), "java -Xmx12G -jar /opt/tools/purple/");
    }

    static InputBundle createInput(String set, String tumor, String reference) {
        List<InputFileDescriptor> inputs = Lists.newArrayList();
        inputs.add(createFileDescription("set", set));
        inputs.add(createFileDescription("tumor_sample", tumor));
        inputs.add(createFileDescription("ref_sample", reference));
        return new InputBundle(inputs);
    }

    static InputFileDescriptor createFileDescription(String key, String value) {
        return ImmutableInputFileDescriptor.builder().billedProject("test").name(key).inputValue(value).build();
    }

}
