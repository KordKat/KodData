package hello1.koddata.engine;

//Inheritance
//Strategy Pattern: สำหรับการเลือกใช้ algorithm ต่างๆ
public class ReplaceOperation implements QueryOperation {

    private final String target;
    private final String replacement;

    public ReplaceOperation(String target, String replacement) {
        this.target = target;
        this.replacement = replacement;
    }

    //Polymorphism
    @Override
    public Value<?> operate(Value<?> value){
        if (value == null || value.get() == null) return value;

        if (value.get() instanceof String s)
            return new Value<>(s.replace(target, replacement));

        return value;
    }
}
