package hello1.koddata.engine;

//Inheritance
public class NullValue extends Value<Object> {

    public NullValue(Object value) {
        super(value);
    }

    //Polymorphism
    @Override
    public String toString() {
        return "Null";
    }
}
