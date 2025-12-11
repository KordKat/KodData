package hello1.koddata.engine;

//Inheritance
//Strategy Pattern: สำหรับการเลือกใช้ algorithm ต่างๆ

public class RepeatOperation implements QueryOperation {

    private final int times;

    public RepeatOperation(int times){
        this.times = times;
    }

    //Polymorphism
    @Override
    public Value<?> operate(Value<?> value){
        if (value == null || value.get() == null) return value;

        if (value.get() instanceof String s)
            return new Value<>(s.repeat(times));

        return value;
    }
}
