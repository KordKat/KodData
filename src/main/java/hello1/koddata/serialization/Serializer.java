package hello1.koddata.serialization;

import hello1.koddata.net.Node;

import java.io.Serializable;

public interface Serializer {

    byte[] serialize(Serializable serializable);

}
