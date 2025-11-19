package hello1.koddata.dataframe.loader;

import hello1.koddata.dataframe.Column;
import hello1.koddata.dataframe.DataFrame;
import hello1.koddata.dataframe.DataFrameSchema;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class JsonLoader extends DataFrameLoader {

    @Override
    public void load(InputStream in) {
        frame = null;
        columns = new Column[0];
        if (in == null) return;

        try {
            byte[] bytes = in.readAllBytes();
            String json = new String(bytes, StandardCharsets.UTF_8);

            int len = bytes.length == 0 ? 1 : bytes.length;
            ByteBuffer buf = ByteBuffer.allocate(len);
            buf.put(bytes, 0, Math.min(bytes.length, len));
            buf.position(0);

            boolean[] notNull = new boolean[]{true};

            columns = new Column[1];
            columns[0] = new Column("json", len, "default", buf, notNull, len);

            frame = new DataFrame(new DataFrameSchema(
                    new String[]{"json"},
                    new int[]{len}
            ));
        } catch (Exception e) {
            frame = null;
            columns = new Column[0];
        }
    }
}
