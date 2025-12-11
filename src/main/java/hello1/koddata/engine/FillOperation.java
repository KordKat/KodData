package hello1.koddata.engine;

//Inheritance
public class FillOperation implements QueryOperation {

    private Value<?> value;
    private Value<?> fillValue;

    public FillOperation(Value<?> value, Value<?> fillValue){
        this.value = value;
        this.fillValue = fillValue;
    }

    //Polymorphism
    @Override
    public Value<?> operate(Value<?> value) {
        return value.get().equals(this.value.get()) ? fillValue : value;
    }
}