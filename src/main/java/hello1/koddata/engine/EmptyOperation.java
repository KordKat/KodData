package hello1.koddata.engine;

public class EmptyOperation implements QueryOperation {

    @Override
    public Value<?> operate(Value<?> value) {
        return value;
    }
}
