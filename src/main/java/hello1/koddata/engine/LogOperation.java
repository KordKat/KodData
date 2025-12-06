package hello1.koddata.engine;

public class LogOperation implements QueryOperation {
    @Override
    public Value<?> operate(Value<?> value) {
        if (value == null || value.get() == null) {
            return value;
        }
        if (!(value.get() instanceof Number number)) {
            return value;
        }
        double val = number.doubleValue();
        if (val <= 0) {
            return value;
        }
        return new Value<>(Math.log(val));
    }
}