package hello1.koddata.engine;
public class ReverseOperation implements QueryOperation {

    @Override
    public Value<?> operate(Value<?> value){
        if (value == null || value.get() == null) return value;

        if (value.get() instanceof String s)
            return new Value<>(new StringBuilder(s).reverse().toString());

        return value;
    }
}
