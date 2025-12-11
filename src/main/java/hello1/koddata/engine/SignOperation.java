package hello1.koddata.engine;

//Inheritance
//Strategy Pattern: สำหรับการเลือกใช้ algorithm ต่างๆ

public class SignOperation implements QueryOperation {

    //Polymorphism
    @Override
    public Value<?> operate(Value<?> value) {
        if (value == null || value.get() == null) {
            return value;
        }
        if (!(value.get() instanceof Number number)) {
            return value;
        }

        double val = number.doubleValue();
        if (val > 0) return new Value<>(1.0);
        if (val < 0) return new Value<>(-1.0);
        return new Value<>(0.0);
    }
}