package hello1.koddata.engine;

//Inheritance
public class ToStringOperation implements QueryOperation {

    //Polymorphism
    @Override
    public Value<?> operate(Value<?> value){
        if (value == null) return null;
        if (value.get() == null) return new Value<>("null");

        return new Value<>(value.get().toString());
    }
}
