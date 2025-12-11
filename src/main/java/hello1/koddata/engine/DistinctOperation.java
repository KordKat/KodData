package hello1.koddata.engine;

import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;

import java.util.*;

//Inheritance
//Strategy Pattern: สำหรับการเลือกใช้ algorithm ต่างๆ
public class DistinctOperation implements ColumnOperation {

    //Polymorphism
    @Override
    public Value<?> operate(Value<?> value) throws KException {

        if (!(value.get() instanceof List<?> column)) {
            throw new KException(ExceptionCode.KD00005, "Only list is accept");
        }
        Set<Object> seen = new HashSet<>();
        List<Integer> indexList = new ArrayList<>();

        for (int i = 0; i < column.size(); i++) {

            Object o = column.get(i);
            if (!(o instanceof Value<?> cell)) {
                continue;
            }

            Object item = cell.get();
            if (seen.add(item)) {
                indexList.add(i);
            }
        }
        return new Value<>(indexList);
    }
}
