package hmf.pipeline.adam;

import org.bdgenomics.adam.api.java.JavaADAMContext;
import org.bdgenomics.adam.rdd.ADAMContext;

import hmf.pipeline.Configuration;
import hmf.pipeline.Stage;

class ADAMStages {

    static Stage bwaPipe(Configuration configuration, ADAMContext adamContext) {
        return new BwaPipe(configuration, adamContext);
    }

    static Stage coordinateSort(Configuration configuration, JavaADAMContext javaADAMContext) {
        return new CoordinateSortADAM(configuration, javaADAMContext);
    }
}
