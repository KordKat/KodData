package hello1.koddata.engine;

import java.util.List;

public class IsListOperation implements QueryOperation{
    @Override
    public Value<?> operate(Value<?> value) {
        if(value.get() instanceof List){
            return new Value<>(Boolean.TRUE);
        }
        return new Value<>(Boolean.FALSE);
    }
}
