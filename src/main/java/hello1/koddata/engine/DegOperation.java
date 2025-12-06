package hello1.koddata.engine;

public class DegOperation implements QueryOperation{
    @Override
    public Value<?> operate(Value<?> value) {
        if (value == null || value.get() == null)
            return value;
        if(!(value.get() instanceof Number number)){
            return value;
        }
        return new Value<>(Math.toDegrees(number.doubleValue()));
    }
}
