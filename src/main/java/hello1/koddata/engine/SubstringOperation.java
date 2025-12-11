package hello1.koddata.engine;

//Inheritance
//Strategy Pattern: สำหรับการเลือกใช้ algorithm ต่างๆ

public class SubstringOperation implements QueryOperation {
    private final int start;
    private final int end;

    public SubstringOperation(int start, int end){
        this.start = start;
        this.end = end;
    }

    //Polymorphism
    @Override
    public Value<?> operate(Value<?> value){
        if (value == null || value.get() == null) return value;
        if (!(value.get() instanceof String s)) return value;

        return new Value<>(s.substring(start, end));
    }
}
