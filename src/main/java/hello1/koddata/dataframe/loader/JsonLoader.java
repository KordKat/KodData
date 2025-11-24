package hello1.koddata.dataframe.loader;

import hello1.koddata.dataframe.*;
import hello1.koddata.exception.KException;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.util.*;


public class JsonLoader extends DataFrameLoader {

    private final String memoryGroupName;

    public JsonLoader(String memoryGroupName) {
        this.memoryGroupName = memoryGroupName;
    }

    @Override
    public void load(InputStream in) {
        try {
            String json = readAll(in);

            // แปลงเป็น List<Map<String,Object>>
            List<Map<String,Object>> rows = parseJsonArray(json);
            if (rows.isEmpty()) return;

            List<String> keys = new ArrayList<>(rows.get(0).keySet());
            int colCount = keys.size();
            int rowCount = rows.size();
            columns = new Column[colCount];

            String[] schemaNames = new String[colCount];
            int[] schemaSizes = new int[colCount];

            for (int c = 0; c < colCount; c++) {
                String colName = keys.get(c);

                List<Object> colVals = new ArrayList<>();
                for (Map<String,Object> r : rows) colVals.add(r.get(colName));

                ColumnType type = detectType(colVals);
                Column col = buildColumn(colName, colVals, type);

                columns[c] = col;
                schemaNames[c] = colName;
                schemaSizes[c] = getSchemaSize(type);
            }

            frame = new DataFrame(new DataFrameSchema(schemaNames, schemaSizes), "json");

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String readAll(InputStream in) throws Exception {
        byte[] data = in.readAllBytes();
        return new String(data, StandardCharsets.UTF_8).trim();
    }

    private List<Map<String,Object>> parseJsonArray(String json) {
        json = json.trim();
        if (!json.startsWith("[") || !json.endsWith("]"))
            throw new RuntimeException("JSON must be array");

        json = json.substring(1, json.length()-1).trim();

        List<Map<String,Object>> rows = new ArrayList<>();

        int i = 0;
        while (i < json.length()) {

            // หา object {"a":1}
            int start = json.indexOf("{", i);
            if (start < 0) break;

            int brace = 1;
            int j = start + 1;

            // หา } ที่เป็นคู่ของ {
            while (j < json.length() && brace > 0) {
                char c = json.charAt(j);
                if (c == '{') brace++;
                else if (c == '}') brace--;
                j++;
            }

            String obj = json.substring(start + 1, j - 1).trim();
            rows.add(parseObject(obj));

            // ขยับ pointer
            i = j;
        }
        return rows;
    }

    private Map<String,Object> parseObject(String obj) {
        Map<String,Object> map = new LinkedHashMap<>();

        int i = 0;
        while (i < obj.length()) {

            // key ต้องเป็น "abc"
            int q1 = obj.indexOf('"', i);
            int q2 = obj.indexOf('"', q1 + 1);

            String key = obj.substring(q1 + 1, q2);

            // หา :
            int colon = obj.indexOf(':', q2);
            int valStart = colon + 1;

            // อ่าน value
            ParseResult r = parseValue(obj, valStart);
            map.put(key, r.value);

            i = r.nextPos;

            // ข้าม comma
            if (i < obj.length() && obj.charAt(i) == ',') i++;
            while (i < obj.length() && Character.isWhitespace(obj.charAt(i))) i++;
        }

        return map;
    }

    private static class ParseResult {
        Object value;
        int nextPos;
    }

    private ParseResult parseValue(String s, int i) {
        ParseResult r = new ParseResult();

        while (i < s.length() && Character.isWhitespace(s.charAt(i))) i++;
        char c = s.charAt(i);

        // string
        if (c == '"') {
            int q2 = s.indexOf('"', i + 1);
            r.value = s.substring(i + 1, q2);
            r.nextPos = q2 + 1;
            return r;
        }

        // list
        if (c == '[') {
            int bracket = 1;
            int j = i + 1;

            while (j < s.length() && bracket > 0) {
                char ch = s.charAt(j);
                if (ch == '[') bracket++;
                else if (ch == ']') bracket--;
                j++;
            }

            String arr = s.substring(i + 1, j - 1).trim();
            r.value = parseList(arr);
            r.nextPos = j;
            return r;
        }

        // boolean
        if (s.startsWith("true", i)) {
            r.value = true;
            r.nextPos = i + 4;
            return r;
        }
        if (s.startsWith("false", i)) {
            r.value = false;
            r.nextPos = i + 5;
            return r;
        }

        // null
        if (s.startsWith("null", i)) {
            r.value = null;
            r.nextPos = i + 4;
            return r;
        }

        // number
        int j = i;
        while (j < s.length() && "0123456789+-.eE".indexOf(s.charAt(j)) >= 0) j++;
        String num = s.substring(i, j);

        if (num.contains(".") || num.contains("e") || num.contains("E")) {
            r.value = Double.parseDouble(num);
        } else {
            try { r.value = Integer.parseInt(num); }
            catch(Exception e) { r.value = Long.parseLong(num); }
        }

        r.nextPos = j;
        return r;
    }

    private List<Object> parseList(String arr) {
        List<Object> list = new ArrayList<>();
        int i = 0;
        while (i < arr.length()) {

            ParseResult r = parseValue(arr, i);
            list.add(r.value);

            i = r.nextPos;
            if (i < arr.length() && arr.charAt(i) == ',') i++;
            while (i < arr.length() && Character.isWhitespace(arr.charAt(i))) i++;
        }
        return list;
    }

    // TYPE DETECTION
    private enum ColumnType {
        INT, LONG, DOUBLE, BOOLEAN,
        STRING,
        DATE, TIMESTAMP,
        LIST_INT, LIST_LONG, LIST_DOUBLE,
        LIST_STRING,
        LIST_DATE, LIST_TIMESTAMP,
        LIST_MIXED
    }

    private ColumnType detectType(List<Object> values) {
        boolean allInt = true, allLong = true, allDouble = true;
        boolean allBool = true, allString = true;
        boolean allDate = true, allTimestamp = true;
        boolean allList = true;

        boolean listAllInt = true, listAllLong = true, listAllDouble = true;
        boolean listAllString = true, listAllDate = true, listAllTimestamp = true;

        for (Object v : values) {
            if (v == null) continue;

            if (!(v instanceof Integer)) allInt = false;
            if (!(v instanceof Long)) allLong = false;
            if (!(v instanceof Double)) allDouble = false;
            if (!(v instanceof Boolean)) allBool = false;
            if (!(v instanceof String)) allString = false;

            if (v instanceof String && allDate) {
                try { LocalDate.parse(v.toString()); }
                catch(Exception e){ allDate = false; }
            }

            if (v instanceof String && allTimestamp) {
                try { Instant.parse(v.toString()); }
                catch(Exception e){ allTimestamp = false; }
            }

            if (!(v instanceof List)) allList = false;
            else {
                List<?> list = (List<?>) v;

                for (Object e : list) {
                    if (e == null) continue;

                    if (!(e instanceof Integer)) listAllInt = false;
                    if (!(e instanceof Long)) listAllLong = false;
                    if (!(e instanceof Double)) listAllDouble = false;
                    if (!(e instanceof String)) listAllString = false;

                    if (e instanceof String && listAllDate) {
                        try { LocalDate.parse(e.toString()); }
                        catch(Exception ex){ listAllDate = false; }
                    }

                    if (e instanceof String && listAllTimestamp) {
                        try { Instant.parse(e.toString()); }
                        catch(Exception ex){ listAllTimestamp = false; }
                    }
                }
            }
        }

        if (allInt) return ColumnType.INT;
        if (allLong) return ColumnType.LONG;
        if (allDouble) return ColumnType.DOUBLE;
        if (allBool) return ColumnType.BOOLEAN;
        if (allDate) return ColumnType.DATE;
        if (allTimestamp) return ColumnType.TIMESTAMP;
        if (allString) return ColumnType.STRING;

        if (allList) {
            if (listAllInt) return ColumnType.LIST_INT;
            if (listAllLong) return ColumnType.LIST_LONG;
            if (listAllDouble) return ColumnType.LIST_DOUBLE;
            if (listAllString) return ColumnType.LIST_STRING;
            if (listAllDate) return ColumnType.LIST_DATE;
            if (listAllTimestamp) return ColumnType.LIST_TIMESTAMP;
            return ColumnType.LIST_MIXED;
        }

        return ColumnType.STRING;
    }

    private int getSchemaSize(ColumnType type) {
        return switch (type) {
            case INT -> 4;
            case LONG -> 8;
            case DOUBLE -> 8;
            case BOOLEAN -> 1;
            case DATE -> 8;
            case TIMESTAMP -> 8;
            default -> -1;
        };
    }

    private Column buildColumn(String name, List<Object> vals, ColumnType type) throws KException {

        int rows = vals.size();
        boolean[] flags = new boolean[rows];

        ColumnAllocator alloc;

        if (type == ColumnType.INT || type == ColumnType.LONG || type == ColumnType.DOUBLE ||
                type == ColumnType.BOOLEAN || type == ColumnType.DATE || type == ColumnType.TIMESTAMP) {

            int size = getSchemaSize(type);
            ByteBuffer buf = ByteBuffer.allocate(size * rows);

            for (int i = 0; i < rows; i++) {
                Object v = vals.get(i);

                if (v != null) {
                    flags[i] = true;

                    switch (type) {
                        case INT -> buf.putInt((Integer) v);
                        case LONG -> buf.putLong((Long) v);
                        case DOUBLE -> buf.putDouble((Double) v);
                        case BOOLEAN -> buf.put((byte) ((boolean)v ? 1 : 0));

                        case DATE -> {
                            LocalDate d = LocalDate.parse(v.toString());
                            buf.putLong(d.toEpochDay());
                        }

                        case TIMESTAMP -> {
                            Instant t = Instant.parse(v.toString());
                            buf.putLong(t.toEpochMilli());
                        }
                    }

                } else {
                    buf.position(buf.position() + size);
                }
            }

            return new Column(name, size, memoryGroupName, buf, flags, size, 0, rows - 1);
        }

        else if (type == ColumnType.STRING) {

            List<VariableElement> list = new ArrayList<>();
            for (int i=0; i<rows; i++) {
                Object v = vals.get(i);

                if (v != null) {
                    flags[i] = true;
                    list.add(VariableElement.newStringElement(v.toString()));
                } else {
                    list.add(VariableElement.newElement(new byte[0]));
                }
            }

            return new Column(name, list, memoryGroupName, flags, 0, rows - 1);
        }

        else if (type == ColumnType.LIST_INT || type == ColumnType.LIST_LONG ||
                type == ColumnType.LIST_DOUBLE || type == ColumnType.LIST_DATE ||
                type == ColumnType.LIST_TIMESTAMP) {

            int elementSize = switch (type) {
                case LIST_INT -> 4;
                case LIST_LONG -> 8;
                case LIST_DOUBLE -> 8;
                case LIST_DATE -> 8;
                case LIST_TIMESTAMP -> 8;
                default -> throw new IllegalStateException();
            };

            List<List<byte[]>> lists = new ArrayList<>();
            List<boolean[]> perFlags = new ArrayList<>();

            for (int i=0; i<rows; i++) {
                Object row = vals.get(i);

                if (row == null) {
                    flags[i] = false;
                    lists.add(Collections.emptyList());
                    perFlags.add(new boolean[0]);
                    continue;
                }

                flags[i] = true;

                List<?> input = (List<?>) row;
                boolean[] rowNulls = new boolean[input.size()];
                List<byte[]> converted = new ArrayList<>();

                for (int j=0; j<input.size(); j++) {
                    Object e = input.get(j);
                    ByteBuffer bb = ByteBuffer.allocate(elementSize);

                    if (e != null) {
                        rowNulls[j] = true;

                        switch (type) {
                            case LIST_INT -> bb.putInt((Integer) e);
                            case LIST_LONG -> bb.putLong((Long) e);
                            case LIST_DOUBLE -> bb.putDouble((Double) e);

                            case LIST_DATE -> {
                                LocalDate d = LocalDate.parse(e.toString());
                                bb.putLong(d.toEpochDay());
                            }

                            case LIST_TIMESTAMP -> {
                                Instant t = Instant.parse(e.toString());
                                bb.putLong(t.toEpochMilli());
                            }
                        }

                        converted.add(bb.array());
                    } else {
                        converted.add(new byte[elementSize]);
                    }
                }

                lists.add(converted);
                perFlags.add(rowNulls);
            }

            return new Column(name, memoryGroupName, lists, perFlags, flags, elementSize, 0, rows - 1);
        }

        else {

            List<List<VariableElement>> lists = new ArrayList<>();
            List<boolean[]> perFlags = new ArrayList<>();

            for (int i = 0; i < rows; i++) {
                Object v = vals.get(i);

                if (v == null) {
                    flags[i] = false;
                    lists.add(Collections.emptyList());
                    perFlags.add(new boolean[0]);
                    continue;
                }

                flags[i] = true;

                List<?> input = (List<?>) v;
                boolean[] rowNulls = new boolean[input.size()];
                List<VariableElement> conv = new ArrayList<>();

                for (int j = 0; j < input.size(); j++) {
                    Object e = input.get(j);

                    if (e != null) {
                        rowNulls[j] = true;
                        conv.add(VariableElement.newStringElement(e.toString()));
                    } else {
                        conv.add(VariableElement.newElement(new byte[0]));
                    }
                }

                lists.add(conv);
                perFlags.add(rowNulls);
            }

            return new Column(name, memoryGroupName, lists, perFlags, flags, 0, rows - 1);
        }
    }
}
