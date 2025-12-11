package hello1.koddata.engine;

//Inheritance
public class LowerOperation implements QueryOperation {

    //Polymorphism
    @Override
    public Value<?> operate(Value<?> value) {
        if (value == null || value.get() == null)
            return value;
        if (!(value.get() instanceof String string)) {
            return value;
        }
        return new Value<>(string.toLowerCase());
    }
}