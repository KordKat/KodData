package hello1.koddata.dataframe;

import java.nio.charset.StandardCharsets;

public record VariableElement(byte[] value) {

    public static VariableElement newStringElement(String str) {
        byte[] charBytes = str.getBytes(StandardCharsets.UTF_8);
        return new VariableElement(charBytes);
    }

    public static VariableElement newElement(byte[] bytes, int length) {
        byte[] copy = new byte[length];
        System.arraycopy(bytes, 0, copy, 0, length);
        return new VariableElement(copy);
    }

    public static VariableElement newElement(byte[] bytes) {
        return new VariableElement(bytes.clone());
    }

    @Override
    public String toString() {
        return new String(value, StandardCharsets.UTF_8);
    }
}

