package hello1.koddata.engine;

public class CoalesceOperation implements QueryOperation{
    private Value<?> fillValue;

    public  CoalesceOperation(Value<?> fillValue){
        this.fillValue = fillValue;
    }

    @Override
    public Value<?> operate(Value<?> value) {

        if(value.get() == null){
            return fillValue;
        }
        return value;
    }
}
