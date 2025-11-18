package hello1.koddata.engine;

public class SinOperation implements QueryOperation {
    @Override
    public Value<?> operate(Value<?> value) {
        if (value == null || value.get() == null) {
            return null;
        }
        if (!(value.get() instanceof Number number)) {
            return null;
        }
        return new Value<>(Math.sin(number.doubleValue()));
    }
}