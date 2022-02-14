package com.hartwig.pipeline.tertiary.lilac;

import com.hartwig.pipeline.execution.vm.SambambaCommand;
import com.hartwig.pipeline.resource.ResourceFiles;

public class LilacBamSliceCommand extends SambambaCommand {
    LilacBamSliceCommand(ResourceFiles resourceFiles, String inputBam, String outputBam) {
        super("slice", "-L", resourceFiles.hlaRegionBed(), "-o", outputBam, inputBam);
    }
}
