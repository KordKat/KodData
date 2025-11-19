package hello1.koddata.dataframe.loader;

import hello1.koddata.dataframe.Column;
import hello1.koddata.dataframe.DataFrame;
import hello1.koddata.dataframe.DataFrameSchema;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class CSVLoader extends DataFrameLoader {

    @Override
    public void load(InputStream in) {
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String[] names = br.readLine().split(",");
            List<String[]> rows = new ArrayList<>();

            String line;
            while ((line = br.readLine()) != null)
                rows.add(line.split(",", -1));

            int colCount = names.length;
            int rowCount = rows.size();
            columns = new Column[colCount];
            int[] sizes = new int[colCount];

            for (int c = 0; c < colCount; c++) {
                int max = 1;
                for (String[] r : rows)
                    max = Math.max(max, r[c].getBytes(StandardCharsets.UTF_8).length);

                sizes[c] = max;
                ByteBuffer buf = ByteBuffer.allocate(max * rowCount);
                boolean[] notNull = new boolean[rowCount];

                for (int i = 0; i < rowCount; i++) {
                    byte[] b = rows.get(i)[c].getBytes(StandardCharsets.UTF_8);
                    buf.position(i * max);
                    buf.put(b, 0, b.length);
                    notNull[i] = b.length > 0;
                }

                buf.position(0);
                columns[c] = new Column(names[c], max, "default", buf, notNull, max);
            }

            frame = new DataFrame(new DataFrameSchema(names, sizes));

        } catch (Exception e) {
            frame = null;
            columns = new Column[0];
        }
    }
}