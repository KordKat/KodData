package hello1.koddata.sessions;

import hello1.koddata.utils.ref.Reference;
import hello1.koddata.utils.ref.UniqueReference;

public interface KodValue<T> {

    UniqueReference<T> referent();

}
