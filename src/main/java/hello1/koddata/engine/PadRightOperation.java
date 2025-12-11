package hello1.koddata.engine;

//Inheritance
public class PadRightOperation implements QueryOperation {

    private final int length;
    private final char pad;

    public PadRightOperation(int length, char pad){
        this.length = length;
        this.pad = pad;
    }

    //Polymorphism
    @Override
    public Value<?> operate(Value<?> value){
        if (value == null || value.get() == null) return value;

        if (value.get() instanceof String s) {
            if (s.length() >= length) return value;

            int diff = length - s.length();
            return new Value<>(s + String.valueOf(pad).repeat(diff));
        }

        return value;
    }
}
