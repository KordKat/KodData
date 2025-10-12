package hello1.koddata.concurrent;

import java.util.concurrent.atomic.AtomicInteger;

public class ThreadLocalMap {

    private static final java.lang.ThreadLocal<ThreadLocalMap> slowThreadLocal = new java.lang.ThreadLocal<>();
    private static final AtomicInteger indexCounter = new AtomicInteger();
    public static final int VARIABLES_TO_REMOVE_INDEX = countIndex();
    private static final int DEFAULT_ARRAY_LIST_INITIAL_CAPACITY = 8;
    private static final int ARRAY_LIST_CAPACITY_EXPAND_THRESHOLD = 1 << 30;
    // Reference: https://hg.openjdk.java.net/jdk8/jdk8/jdk/file/tip/src/share/classes/java/util/ArrayList.java#l229
    private static final int ARRAY_LIST_CAPACITY_MAX_SIZE = Integer.MAX_VALUE - 8;

    public static final Object UNSET = new Object();
    private Object[] indexedVariables;

    public static ThreadLocalMap getIfSet() {
        Thread thread = Thread.currentThread();
        if(thread instanceof ThreadLocalThread tt){

        }
        return null;
    }

    public static int countIndex(){
        int index = indexCounter.getAndIncrement();
        if(index >= ARRAY_LIST_CAPACITY_MAX_SIZE || index < 0){
            indexCounter.set(ARRAY_LIST_CAPACITY_MAX_SIZE);
            throw new IllegalStateException("thread local map index out of bound");
        }
        return index;
    }

}
