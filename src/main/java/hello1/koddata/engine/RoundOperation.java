package hello1.koddata.engine;

public class RoundOperation implements QueryOperation {
    @Override
    public Value<?> operate(Value<?> value) {
        if (value == null || value.get() == null) {
            return value;
        }
        if (!(value.get() instanceof Number number)) {
            return value;
        }
        return new Value<>((double) Math.round(number.doubleValue()));
    }
}