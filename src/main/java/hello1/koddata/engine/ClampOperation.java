package hello1.koddata.engine;

public class ClampOperation implements QueryOperation {
    private final double min, max;

    public ClampOperation(double min, double max){
        this.min = min;
        this.max = max;
    }

    @Override
    public Value<?> operate(Value<?> value) {
        if (value == null || value.get() == null) return value;
        if (!(value.get() instanceof Number n)) return value;

        double v = n.doubleValue();
        if (v < min) v = min;
        else if (v > max) v = max;
        return new Value<>(v);
    }
}
