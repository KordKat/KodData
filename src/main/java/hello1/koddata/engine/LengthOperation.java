package hello1.koddata.engine;


import java.util.List;

//Inheritance
public class LengthOperation implements QueryOperation {

    //Polymorphism
    @Override
    public Value<?> operate(Value<?> value) {
        if (value == null || value.get() == null)
            return value;
        Object object = value.get();
        if (object instanceof String string) {
            return new Value<>((double) string.length());

        }
        if(object instanceof List<?> list){
            return new Value<>((double)list.size());
        }
        return value;
    }
}