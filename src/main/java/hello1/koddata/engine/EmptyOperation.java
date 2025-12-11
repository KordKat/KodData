package hello1.koddata.engine;

//Inheritance
//Strategy Pattern: สำหรับการเลือกใช้ algorithm ต่างๆ
public class EmptyOperation implements QueryOperation {

    //Polymorphism
    @Override
    public Value<?> operate(Value<?> value) {
        return value;
    }
}