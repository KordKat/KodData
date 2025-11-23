package hello1.koddata.dataframe.loader;

import hello1.koddata.dataframe.Column;
import hello1.koddata.dataframe.DataFrame;
import hello1.koddata.dataframe.DataFrameSchema;
import hello1.koddata.dataframe.VariableElement;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.InputStream;
import java.util.*;

public class JsonLoader extends DataFrameLoader {

    private final String memoryGroupName;

    public JsonLoader(String memoryGroupName) {
        this.memoryGroupName = memoryGroupName;
    }

    @Override
    public void load(InputStream in) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            List<Map<String,Object>> rows = mapper.readValue(in, List.class);
            if (rows.isEmpty()) return;

            Map<String,Object> first = rows.get(0);
            List<String> keysList = new ArrayList<>(first.keySet());
            int colCount = keysList.size();

            columns = new Column[colCount];
            String[] keys = new String[colCount];
            int[] sizes = new int[colCount];

            for (int c = 0; c < colCount; c++) {
                String colName = keysList.get(c);
                boolean[] flags = new boolean[rows.size()];
                List<VariableElement> list = new ArrayList<>(rows.size());

                for (int r = 0; r < rows.size(); r++) {
                    Object v = rows.get(r).get(colName);
                    if (v != null) {
                        flags[r] = true;
                        list.add(VariableElement.newStringElement(v.toString()));
                    } else {
                        list.add(VariableElement.newElement(new byte[0]));
                    }
                }

                Column col = new Column(colName, list, memoryGroupName, flags, 0, rows.size());
                columns[c] = col;
                keys[c] = colName;
                sizes[c] = -1;
            }

            DataFrameSchema schema = new DataFrameSchema(keys, sizes);
            frame = new DataFrame(schema, "json");

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
