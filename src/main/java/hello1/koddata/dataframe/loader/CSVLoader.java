package hello1.koddata.dataframe.loader;

import hello1.koddata.dataframe.Column;
import hello1.koddata.dataframe.DataFrame;
import hello1.koddata.dataframe.DataFrameSchema;
import hello1.koddata.dataframe.VariableElement;
import hello1.koddata.exception.KException;
import hello1.koddata.io.BufferedInputStreamPromax;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class CSVLoader extends DataFrameLoader {
    private String memoryGroupName;

    public CSVLoader(String memoryGroupName) {
        this.memoryGroupName = memoryGroupName;
    }

    @Override
    public void load(InputStream in) {
        try (BufferedInputStreamPromax bufferedInputStreamPromax = new BufferedInputStreamPromax(in, 8192)) {

            String text = bufferedInputStreamPromax.readUTF();

            if (text == null || text.isEmpty()) {
                this.columns = new Column[0];
                this.frame = new DataFrame();
                return;
            }

            String[] lines = text.split("\\r?\\n");
            if (lines.length == 0) {
                this.columns = new Column[0];
                this.frame = new DataFrame();
                return;
            }

            String[] headers = lines[0].split(",");
            int columnCount = headers.length;
            int rowCount = lines.length - 1;

            @SuppressWarnings("unchecked")
            List<VariableElement>[] valuesPerColumn = new ArrayList[columnCount];
            boolean[][] notNullPerColumn = new boolean[columnCount][rowCount];

            for (int c = 0; c < columnCount; c++) {
                valuesPerColumn[c] = new ArrayList<>(rowCount);
            }

            int dataRowIndex = 0;
            for (int i = 1; i < lines.length; i++) {
                String line = lines[i].trim();
                if (line.isEmpty()) continue;

                String[] parts = line.split(",");
                for (int c = 0; c < columnCount; c++) {
                    String cell = c < parts.length ? parts[c].trim() : "";
                    if (cell.isEmpty()) {
                        notNullPerColumn[c][dataRowIndex] = false;
                        valuesPerColumn[c].add(null);
                    } else {
                        notNullPerColumn[c][dataRowIndex] = true;
                        byte[] bytes = cell.getBytes(StandardCharsets.UTF_8);
                        VariableElement ve = new VariableElement(bytes);
                        valuesPerColumn[c].add(ve);
                    }
                }
                dataRowIndex++;
            }

            columns = new Column[columnCount];
            for (int c = 0; c < columnCount; c++) {
                String name = headers[c].trim();
                boolean[] notNullFlags = notNullPerColumn[c];
                List<VariableElement> values = valuesPerColumn[c];

                columns[c] = new Column(
                        name,
                        values,
                        memoryGroupName,
                        notNullFlags,
                        0,
                        dataRowIndex
                );
            }

            DataFrameSchema schema = new DataFrameSchema();
            this.frame = new DataFrame(schema);
            if (columnCount > 0 && dataRowIndex > 0) {

                int elementSize = 8;
                ByteBuffer fixedBuffer = ByteBuffer.allocate(elementSize * dataRowIndex);
                boolean[] fixedNotNull = new boolean[dataRowIndex];
                for (int r = 0; r < dataRowIndex; r++) {
                    fixedNotNull[r] = true;
                    fixedBuffer.putLong(0L);
                }
                fixedBuffer.flip();

                Column fixedColumn = new Column(
                        "fixedSample",
                        elementSize,
                        memoryGroupName,
                        fixedBuffer,
                        fixedNotNull,
                        elementSize,
                        0,
                        dataRowIndex
                );

                List<List<byte[]>> fixedLists = new ArrayList<>();
                List<boolean[]> perListNotNull = new ArrayList<>();
                boolean[] columnNotNullFlags = new boolean[dataRowIndex];
                for (int r = 0; r < dataRowIndex; r++) {
                    List<byte[]> rowList = new ArrayList<>();
                    rowList.add(new byte[] { (byte) r });
                    fixedLists.add(rowList);

                    boolean[] flags = new boolean[1];
                    flags[0] = true;
                    perListNotNull.add(flags);

                    columnNotNullFlags[r] = true;
                }

                Column fixedListColumn = new Column(
                        "fixedListSample",
                        memoryGroupName,
                        fixedLists,
                        perListNotNull,
                        columnNotNullFlags,
                        1,
                        0,
                        dataRowIndex
                );

                List<List<VariableElement>> variableLists = new ArrayList<>();
                List<boolean[]> perListNotNull2 = new ArrayList<>();
                boolean[] columnNotNullFlags2 = new boolean[dataRowIndex];
                for (int r = 0; r < dataRowIndex; r++) {
                    List<VariableElement> rowList = new ArrayList<>();
                    rowList.add(new VariableElement(
                            ("row" + r).getBytes(StandardCharsets.UTF_8)
                    ));
                    variableLists.add(rowList);

                    boolean[] flags = new boolean[1];
                    flags[0] = true;
                    perListNotNull2.add(flags);

                    columnNotNullFlags2[r] = true;
                }

                Column variableListColumn = new Column(
                        "variableListSample",
                        memoryGroupName,
                        variableLists,
                        perListNotNull2,
                        columnNotNullFlags2,
                        0,
                        dataRowIndex
                );
            }

        } catch (IOException e) {
            throw new RuntimeException("Error reading CSV InputStream", e);
        } catch (KException e) {
            throw new RuntimeException("Error creating Columns from CSV", e);
        }
    }
}