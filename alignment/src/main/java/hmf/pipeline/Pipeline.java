package hmf.pipeline;

import hmf.bwa.Bwa;
import hmf.bwa.BwaConfiguration;

class Pipeline {

    private final BwaConfiguration configuration;

    Pipeline(BwaConfiguration configuration) {
        this.configuration = configuration;
    }

    void execute() {
        Bwa bwa = new Bwa(configuration);
        bwa.runTool();
    }
}
