package hello1.koddata.memory;


import hello1.koddata.exception.KException;

public interface Allocator {

    Memory allocate() throws KException;

    void deallocate(Memory ref);

    public static Allocator INT_ALLOCATOR = new Allocator() {
        @Override
        public Memory allocate() throws KException {
            return WritableMemory.allocate(4);
        }

        @Override
        public void deallocate(Memory ref) {
            ref.free();
        }
    };

}
