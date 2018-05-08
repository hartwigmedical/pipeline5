package hmf.pipeline;

import picard.cmdline.CommandLineProgram;

class PicardExecutor {

    private final CommandLineProgram program;
    private final String[] args;
    private final Trace trace;

    private PicardExecutor(final CommandLineProgram program, final String[] args) {
        this.program = program;
        this.args = args;
        this.trace = Trace.of(PicardExecutor.class, String.format("Execution of picard tool %s", program.getClass().getSimpleName()));
    }

    static PicardExecutor of(final CommandLineProgram program, final String[] args) {
        return new PicardExecutor(program, args);
    }

    void execute() {
        trace.start();
        program.instanceMain(args);
        trace.finish();
    }
}
