package com.hartwig.batch.input;

import org.junit.Before;
import org.junit.Test;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class InputFileDescriptorTest {
    private String project;
    private String local;
    private String remote;
    private String name;
    private ImmutableInputFileDescriptor.Builder builder;

    @Before
    public void setup() {
        project = "my-project";
        local = "local_dest";
        remote = "some.remote.file";
        name = "descriptive name";
        builder = InputFileDescriptor.builder().billedProject(project).name(name);
    }

    @Test
    public void shouldAddProtocolIfLeftOutOfFilenames() {
        assertCommandForm(builder.inputValue(format("gs://%s", remote)).build().toCommandForm(local), remote);
    }

    @Test
    public void shouldNotDoubleAddProtocolIfIncludedInFilenames() {
        assertCommandForm(builder.inputValue(remote).build().toCommandForm(local), remote);
    }

    @Test
    public void shouldSetName() {
        assertThat(builder.inputValue(remote).build().name()).isEqualTo(name);
    }

    private void assertCommandForm(String commandForm, String remoteFile) {
        assertThat(commandForm).isEqualTo(format("gsutil -o 'GSUtil:parallel_thread_count=1' -o \"GSUtil:sliced_object_download_max_components=$(nproc)\" -q -u %s cp gs://%s %s", project, remoteFile, local));
    }
}