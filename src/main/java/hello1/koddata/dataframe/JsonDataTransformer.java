package hello1.koddata.dataframe;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import hello1.koddata.engine.Value;

public class JsonDataTransformer implements DataTransformer {

    private final Gson gson = new GsonBuilder()
            .serializeNulls()
            .setPrettyPrinting()
            .create();

    @Override
    public String transform(DataFrameRecord[] records) {
        JsonArray array = new JsonArray();

        for (DataFrameRecord record : records) {
            JsonObject obj = new JsonObject();
            String[] cols = record.getColumns();
            Value<?>[] vals = record.getValues();

            for (int i = 0; i < cols.length; i++) {
                Object value = vals[i].get();

                if (value == null) {
                    obj.add(cols[i], null);
                } else if (value instanceof Number) {
                    obj.addProperty(cols[i], (Number) value);
                } else if (value instanceof Boolean) {
                    obj.addProperty(cols[i], (Boolean) value);
                } else if (value instanceof Character) {
                    obj.addProperty(cols[i], (Character) value);
                } else {
                    obj.addProperty(cols[i], value.toString());
                }
            }

            array.add(obj);
        }

        return gson.toJson(array);
    }
}
