package hello1.koddata.serialization;

import java.io.Serializable;

public interface Serializer {

    byte[] serialize(Serializable serializable);

}
