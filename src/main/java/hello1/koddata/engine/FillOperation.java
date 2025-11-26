package hello1.koddata.engine;

import java.util.function.Predicate;

public class FillOperation implements QueryOperation {

    private Value<?> value;
    private Value<?> fillValue;

    public FillOperation(Value<?> value, Value<?> fillValue){
        this.value = value;
        this.fillValue = fillValue;
    }

    @Override
    public Value<?> operate(Value<?> value) {
        return value.get().equals(this.value.get()) ? fillValue : value;
    }
}