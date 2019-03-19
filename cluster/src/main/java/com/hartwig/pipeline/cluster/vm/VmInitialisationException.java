package com.hartwig.pipeline.cluster.vm;

@SuppressWarnings("serial")
public class VmInitialisationException extends RuntimeException {
    public VmInitialisationException(String message, Throwable cause) {
        super(message, cause);
    }

    public VmInitialisationException(String message) {
        super(message);
    }
}
