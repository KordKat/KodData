package hello1.koddata.engine;

public class ModOperation implements QueryOperation {
    private final double mod;

    public ModOperation(double mod) {
        this.mod = mod;
    }

    @Override
    public Value<?> operate(Value<?> value) {
        if (value == null || value.get() == null) return value;
        if (!(value.get() instanceof Number n)) return value;

        return new Value<>(n.doubleValue() % mod);
    }
}
