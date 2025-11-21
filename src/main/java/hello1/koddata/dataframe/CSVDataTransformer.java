package hello1.koddata.dataframe;

import hello1.koddata.engine.Value;

public class CSVDataTransformer implements DataTransformer {

    @Override
    public String transform(DataFrameRecord[] records) {
        if (records == null || records.length == 0) {
            return "";
        }

        StringBuilder sb = new StringBuilder();

        DataFrameRecord first = records[0];
        String[] columns = first.getColumns();

        for (int i = 0; i < columns.length; i++) {
            sb.append(escapeCsv(columns[i]));
            if (i < columns.length - 1) sb.append(',');
        }
        sb.append('\n');

        for (DataFrameRecord record : records) {
            Value<?>[] values = record.getValues();

            for (int i = 0; i < values.length; i++) {
                Object v = values[i].get();
                sb.append(escapeCsv(v == null ? "" : v.toString()));
                if (i < values.length - 1) sb.append(',');
            }
            sb.append('\n');
        }

        return sb.toString();
    }

    private String escapeCsv(String value) {
        if (value == null) return "";

        boolean needQuotes = value.contains(",") ||
                value.contains("\"") ||
                value.contains("\n") ||
                value.contains("\r");

        if (!needQuotes) {
            return value;
        }

        String escaped = value.replace("\"", "\"\"");

        return "\"" + escaped + "\"";
    }
}
