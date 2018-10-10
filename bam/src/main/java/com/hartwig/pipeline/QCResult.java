package com.hartwig.pipeline;

import org.immutables.value.Value;

@Value.Immutable
public interface QCResult {

    @Value.Parameter
    boolean isOk();

    @Value.Parameter
    String message();

    static QCResult ok() {
        return ImmutableQCResult.of(true, "OK");
    }

    static QCResult failure(String message) {
        return ImmutableQCResult.of(false, message);
    }
}
