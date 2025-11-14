package hello1.koddata.engine;

import java.util.function.Predicate;

public class FillOperation implements QueryOperation {

    private Predicate<Value<?>> predicate;
    private Value<?> fillValue;

    public FillOperation(Predicate<Value<?>> predicate, Value<?> fillValue){
        this.predicate = predicate;
        this.fillValue = fillValue;
    }

    @Override
    public Value<?> operate(Value<?> value) {
        return predicate.test(value) ? fillValue : value;
    }
}