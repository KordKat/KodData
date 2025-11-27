package hello1.koddata.engine;

import hello1.koddata.exception.ExceptionCode;
import hello1.koddata.exception.KException;

import java.util.*;

public class DistinctOperation implements ColumnOperation {

    @Override
    public Value<?> operate(Value<?> value) throws KException {

        if (!(value.get() instanceof List<?> column)) {
            throw new KException(ExceptionCode.KD00005, "Only list is accept");
        }

        // ใช้ set เก็บค่าที่เคยเจอ
        Set<Object> seen = new HashSet<>();

        // เก็บ index ที่ distinct
        List<Integer> indexList = new ArrayList<>();

        // ไล่ตามลำดับใน list
        for (int i = 0; i < column.size(); i++) {

            Object o = column.get(i);

            // element ต้องเป็น Value<?> ถ้าไม่ใช่ก็ข้าม
            if (!(o instanceof Value<?> cell)) {
                continue; // จะโยน error ก็ได้ แต่ขอน้องทำแบบ safe
            }

            Object item = cell.get(); // ค่าจริงใน Value<?>

            // ถ้ายังไม่เคยเจอ ให้เก็บ index ครั้งแรก
            if (seen.add(item)) {
                indexList.add(i);
            }
        }

        // คืน index list
        return new Value<>(indexList);
    }
}
