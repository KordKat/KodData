package hello1.koddata.engine;

import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;

import java.util.ArrayList;
import java.util.List;

//Inheritance
//Strategy Pattern: สำหรับการเลือกใช้ algorithm ต่างๆ

public class SkipOperation implements ColumnOperation {

    private final int count;

    public SkipOperation(int count) {
        this.count = Math.max(0, count);
    }

    //Polymorphism
    @Override
    public Value<?> operate(Value<?> value) throws KException {

        if (!(value.get() instanceof List<?> column)) {
            throw new KException(ExceptionCode.KD00005, "Only list is accept");
        }

        List<Integer> result = new ArrayList<>();

        // index เริ่มหลังจาก skip
        for (int i = count; i < column.size(); i++) {
            result.add(i);
        }

        return new Value<>(result);
    }
}
