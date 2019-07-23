package com.hartwig.pipeline.calling.structural.gridss.stage;

import static org.junit.Assert.fail;

import com.hartwig.pipeline.calling.SubStageTest;
import com.hartwig.pipeline.calling.structural.gridss.CommonEntities;
import com.hartwig.pipeline.execution.vm.BashCommand;

import org.mockito.ArgumentCaptor;

public abstract class GridssSubStageTest extends SubStageTest implements CommonEntities {
    ArgumentCaptor<BashCommand> captor;

    void assertBashContains(final BashCommand model) {
        for (BashCommand actual : captor.getAllValues()) {
            if (actual.getClass().isAssignableFrom(model.getClass())) {
                if (actual.asBash().equals(model.asBash())) {
                    return;
                }
            }
        }
        String message = "Did not find BASH command in substage! Expected:\n" + model.asBash();
        message += "\n\nActual commands:\n";
        for (BashCommand actual: captor.getAllValues()) {
            message += actual.asBash() + "\n";
        }
        fail(message);
    }
}
