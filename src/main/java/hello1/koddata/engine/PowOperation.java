package hello1.koddata.engine;

public class PowOperation implements QueryOperation {
    private final double exponent;

    public PowOperation(double exponent) {
        this.exponent = exponent;
    }

    @Override
    public Value<?> operate(Value<?> value) {
        if (value == null || value.get() == null)
            return value;
        if (!(value.get() instanceof Number n))
            return value;

        return new Value<>(Math.pow(n.doubleValue(), exponent));
    }
}
