package com.hartwig.pipeline.rerun;


import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class PersistedLocationsTest {

    private static final String SET = "set";
    private static final String NAMESPACE = "namespace";
    private static final String BLOB = "blob";
    private static final String SAMPLE = "sample";

    @Test
    public void blobForSet() {
        String result = PersistedLocations.blobForSet(SET, NAMESPACE, BLOB);
        assertThat(result).isEqualTo("set/namespace/blob");
    }

    @Test
    public void blobForSingle() {
        String result = PersistedLocations.blobForSingle(SET, SAMPLE, NAMESPACE, BLOB);
        assertThat(result).isEqualTo("set/sample/namespace/blob");
    }

    @Test
    public void pathForSample() {
        String result = PersistedLocations.pathForSet(SET, NAMESPACE);
        assertThat(result).isEqualTo("set/namespace");
    }

}