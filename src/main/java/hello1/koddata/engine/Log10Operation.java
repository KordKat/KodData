package hello1.koddata.engine;

public class Log10Operation implements QueryOperation {
    @Override
    public Value<?> operate(Value<?> value) {
        if (value == null || value.get() == null) {
            return null;
        }
        if (!(value.get() instanceof Number number)) {
            return null;
        }
        double val = number.doubleValue();
        if (val <= 0) {
            return null;
        }
        return new Value<>(Math.log10(val));
    }
}