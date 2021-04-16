package com.hartwig.pipeline.calling.structural.gridss.stage;

import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.tools.Versions;

public class GridssJar {

    static String path(){
        return VmDirectories.TOOLS + "/gridss/" + Versions.GRIDSS + "/gridss.jar";
    }
}
