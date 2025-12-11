package hello1.koddata.engine;

//Inheritance
public class FloorOperation implements QueryOperation {

    //Polymorphism
    @Override
    public Value<?> operate(Value<?> value) {
        if (value == null || value.get() == null) {
            return value;
        }
        if (!(value.get() instanceof Number number)) {
//            return null;
            return value;
        }
        return new Value<>(Math.floor(number.doubleValue()));
    }
}