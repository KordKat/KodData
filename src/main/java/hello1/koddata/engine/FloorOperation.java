package hello1.koddata.engine;

public class FloorOperation implements QueryOperation {
    @Override
    public Value<?> operate(Value<?> value) {
        if (value == null || value.get() == null) {
            return value;
        }
        if (!(value.get() instanceof Number number)) {
//            return null;
            return value;
        }
        return new Value<>(Math.floor(number.doubleValue()));
    }
}