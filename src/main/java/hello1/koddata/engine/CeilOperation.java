package hello1.koddata.engine;

public class CeilOperation implements QueryOperation {
    @Override
    public Value<?> operate(Value<?> value) {
        if (value == null || value.get() == null) {
            return null;
        }
        if (!(value.get() instanceof Number number)) {
            return null;
        }
        return new Value<>(Math.ceil(number.doubleValue()));
    }
}