package hello1.koddata.memory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MemoryGroup {

    static final Map<String, MemoryGroup> memoryGroups = new ConcurrentHashMap<>();
    private final String name;
    private final Map<Memory, Allocator> referencePool = new ConcurrentHashMap<>();

    static {
        Runtime.getRuntime().addShutdownHook(new MemoryGroupShutdownHookThread());
    }

    private MemoryGroup(String name){
        this.name = name;
    }

    public static String newMemoryGroup(String name){
        if(memoryGroups.containsKey(name)){
            return null;
        }
        MemoryGroup group = new MemoryGroup(name);
        memoryGroups.put(name, group);
        return name;
    }

    public Memory allocate(Allocator allocator){
        Memory allocated = allocator.allocate();
        referencePool.put(allocated, allocator);
        return allocated;
    }

    public Memory allocate(long size){
        return allocate(new SimpleAllocator(size));
    }

    public void deallocate(Memory memory){
        if(!referencePool.containsKey(memory)) return;

        referencePool.get(memory).deallocate(memory);
        referencePool.remove(memory);
    }

    public void deallocateAll(){
        for (Map.Entry<Memory, Allocator> entry : referencePool.entrySet()){
            Memory key = entry.getKey();
            deallocate(key);
        }
    }

    public String getName() {
        return name;
    }

    static class MemoryGroupShutdownHookThread extends Thread {

        @Override
        public void run() {
            for(Map.Entry<String, MemoryGroup> entry : memoryGroups.entrySet()){
                entry.getValue().deallocateAll();
            }
        }
    }

    public static MemoryGroup get(String name){
        return memoryGroups.get(name);
    }

}
