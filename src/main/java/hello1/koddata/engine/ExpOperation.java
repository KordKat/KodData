package hello1.koddata.engine;

public class ExpOperation implements QueryOperation {
    @Override
    public Value<?> operate(Value<?> value) {
        if (value == null || value.get() == null)
            return value;
        if (!(value.get() instanceof Number number)) {
            return value;
        }
        return new Value<>(Math.exp(number.doubleValue()));
    }
}