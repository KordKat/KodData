package hello1.koddata.engine;

public class PadLeftOperation implements QueryOperation {

    private final int length;
    private final char pad;

    public PadLeftOperation(int length, char pad){
        this.length = length;
        this.pad = pad;
    }

    @Override
    public Value<?> operate(Value<?> value){
        if (value == null || value.get() == null) return value;

        if (value.get() instanceof String s) {
            if (s.length() >= length) return value;

            int diff = length - s.length();
            StringBuilder sb = new StringBuilder(length);
            sb.append(String.valueOf(pad).repeat(diff));
            sb.append(s);
            return new Value<>(sb.toString());
        }

        return value;
    }
}
