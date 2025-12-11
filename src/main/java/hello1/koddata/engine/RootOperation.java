package hello1.koddata.engine;

//Inheritance
//Strategy Pattern: สำหรับการเลือกใช้ algorithm ต่างๆ
public class RootOperation implements QueryOperation {
    private final double root;

    public RootOperation(double root) {
        this.root = root;
    }

    //Polymorphism
    @Override
    public Value<?> operate(Value<?> value) {
        if (value == null || value.get() == null) return value;
        if (!(value.get() instanceof Number n)) return value;

        return new Value<>(Math.pow(n.doubleValue(), 1.0 / root));
    }
}
