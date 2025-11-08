package hello1.koddata.memory;


import hello1.koddata.exception.KException;

public interface Allocator {

    Memory allocate() throws KException;

    void deallocate(Memory ref);

}
