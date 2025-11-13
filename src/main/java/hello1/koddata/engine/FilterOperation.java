package hello1.koddata.engine;

import java.util.function.Predicate;

public class FilterOperation implements QueryOperation {

    private Predicate<Value<?>> predicate;

    public FilterOperation(Predicate<Value<?>> predicate){
        this.predicate = predicate;
    }

    @Override
    public Value<?> operate(Value<?> value) {
        return predicate.test(value) ? value : null;
    }
}
