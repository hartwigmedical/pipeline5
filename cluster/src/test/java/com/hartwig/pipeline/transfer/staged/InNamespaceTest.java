package com.hartwig.pipeline.transfer.staged;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.testsupport.TestBlobs;

import org.junit.Before;
import org.junit.Test;

public class InNamespaceTest {

    private InNamespace victim;

    @Before
    public void setUp() throws Exception {
        victim = InNamespace.of("namespace");
    }

    @Test
    public void pathWithNoNamespaceAlwaysFalse() {
        assertThat(victim.test(TestBlobs.blob("no_namespace.txt"))).isFalse();
    }

    @Test
    public void namespaceMatches() {
        assertThat(victim.test(TestBlobs.blob("namespace/in_namespace.txt"))).isTrue();
    }

    @Test
    public void namespaceDoesntMatch() {
        assertThat(victim.test(TestBlobs.blob("another_namespace/not_in_namespace.txt"))).isFalse();
    }
}