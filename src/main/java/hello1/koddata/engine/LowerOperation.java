package hello1.koddata.engine;

public class LowerOperation implements QueryOperation {
    @Override
    public Value<?> operate(Value<?> value) {
        if (value == null || value.get() == null)
            return value;
        if (!(value.get() instanceof String string)) {
            return value;
        }
        return new Value<>(string.toLowerCase());
    }
}