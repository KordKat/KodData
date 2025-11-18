package hello1.koddata.engine;

public class IsNumberOperation implements QueryOperation{



    @Override
    public Value<?> operate(Value<?> value) {
        if(value.get() instanceof Number){
            return new Value<>(Boolean.TRUE);
        }
        return new Value<>(Boolean.FALSE);
    }
}
