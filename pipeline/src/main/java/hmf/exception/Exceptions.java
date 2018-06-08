package hmf.exception;

public class Exceptions {

    public static RuntimeException noLanesInSample() {
        return new IllegalArgumentException("Pre processing requires a sample with at least one lane. "
                + "Check the sample directory and ensure lanes are present and in the convention of {sample}_L00{index}");
    }
}
