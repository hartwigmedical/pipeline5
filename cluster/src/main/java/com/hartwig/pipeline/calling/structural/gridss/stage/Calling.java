package com.hartwig.pipeline.calling.structural.gridss.stage;

import com.hartwig.pipeline.execution.vm.BashCommand;

public class Calling {
    public Calling(CommandFactory commandFactory) {

    }

    public Result initialise() {
        return null;
    }

    public interface Result extends BashCommand {
    }
}
