package hello1.koddata.engine;

public class IsBoolOperation implements QueryOperation{
    @Override
    public Value<?> operate(Value<?> value) {
        if(value.get() instanceof Boolean){
            return new Value<>(Boolean.TRUE);
        }
        return new Value<>(Boolean.FALSE);
    }
}
