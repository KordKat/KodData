package hello1.koddata.engine;

public class IsStringOperation implements QueryOperation{
    @Override
    public Value<?> operate(Value<?> value) {
        if(value.get() instanceof String){
            return new Value<>(Boolean.TRUE);
        }
        return new Value<>(Boolean.FALSE);
    }
}
