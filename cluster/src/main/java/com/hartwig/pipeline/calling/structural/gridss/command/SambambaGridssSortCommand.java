package com.hartwig.pipeline.calling.structural.gridss.command;

import static java.util.Arrays.asList;

import java.util.ArrayList;
import java.util.List;

import com.hartwig.pipeline.calling.command.VersionedToolCommand;
import com.hartwig.pipeline.execution.vm.Bash;
import com.hartwig.pipeline.tools.Versions;

public class SambambaGridssSortCommand extends VersionedToolCommand {
    private SambambaGridssSortCommand(String outputBam, boolean sortByName) {
        super("sambamba", "sambamba", Versions.SAMBAMBA, argsAsArray(outputBam, sortByName));
    }

    private static String[] argsAsArray(String outputBam, boolean sortByName) {
        List<String> args = new ArrayList<>(asList("sort", "-m", "8G", "-t", Bash.allCpus(), "-l", "0", "-o", outputBam));
        if (sortByName) {
            args.add("-n");
        }
        args.add("/dev/stdin");
        return args.toArray(new String[] {});
    }

    public static SambambaGridssSortCommand sortByDefault(final String outputBam) {
        return new SambambaGridssSortCommand(outputBam, false);
    }

    public static SambambaGridssSortCommand sortByName(final String outputBam) {
        return new SambambaGridssSortCommand(outputBam, true);
    }
}
