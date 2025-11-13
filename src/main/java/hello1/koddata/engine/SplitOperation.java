package hello1.koddata.engine;

public class SplitOperation implements QueryOperation {

    @Override
    public Value<?> operate(Value<?> value) {
        return value;
    }
}
