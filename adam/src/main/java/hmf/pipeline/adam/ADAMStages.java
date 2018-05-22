package hmf.pipeline.adam;

import org.bdgenomics.adam.api.java.JavaADAMContext;
import org.bdgenomics.adam.rdd.ADAMContext;

import hmf.pipeline.Stage;
import hmf.sample.Lane;
import hmf.sample.Reference;

class ADAMStages {

    static Stage<Lane> bwaPipe(Reference reference, ADAMContext adamContext) {
        return new BwaPipe(reference, adamContext);
    }

    static Stage<Lane> coordinateSort(JavaADAMContext javaADAMContext) {
        return new CoordinateSortADAM(javaADAMContext);
    }
}
