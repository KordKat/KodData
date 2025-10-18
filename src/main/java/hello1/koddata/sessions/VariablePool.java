package hello1.koddata.sessions;

import hello1.koddata.utils.math.MathUtils;

public class VariablePool {

    private static final float LOAD_FACTOR = 0.75f;

    private String[] varName;
    private KodValue<?>[] values;
    private int[] hashes;
    private int size;
    private int threshold;


    public VariablePool(int initialCapacity){
        int cap = MathUtils.nextPowerOf2(initialCapacity);
        varName = new String[cap];
        values = new KodValue[cap];
        hashes = new int[cap];
        threshold = (int) (cap * LOAD_FACTOR);
    }

    public VariablePool(){
        this(32);
    }

    public void assign(String varN, KodValue<?> value){
        if(size >= threshold) resize();
        int hash = varN.hashCode();
        int idx = hash & (varName.length - 1);

        while(true){
            String k = varName[idx];
            if(k == null){
                varName[idx] = varN;
                values[idx] = value;
                hashes[idx] = hash;
                size++;
                return;
            }else if(hashes[idx] == hash && k.equals(varN)){
                values[idx] = value;
            }
            idx = (idx + 1) & (varName.length - 1);
        }
    }

    public void remove(String varN)  {
        int hash = varN.hashCode();
        int idx = hash & (varName.length - 1);

        while (true) {
            String k = varName[idx];
            if (k == null) return;
            if (hashes[idx] == hash && k.equals(varN)) {
                KodValue<?> kv = values[idx];
                kv.referent().close();
                varName[idx] = null;
                values[idx] = null;
                hashes[idx] = 0;
                size--;

                idx = (idx + 1) & (varName.length - 1);
                while (varName[idx] != null) {
                    String reKey = varName[idx];
                    KodValue<?> reVal = values[idx];
                    int reHash = hashes[idx];

                    varName[idx] = null;
                    values[idx] = null;
                    hashes[idx] = 0;
                    size--;
                    assign(reKey, reVal);
                    idx = (idx + 1) & (varName.length - 1);
                }
                return;
            }
            idx = (idx + 1) & (varName.length - 1);
        }
    }

    public KodValue<?> get(String varN){
        int hash = varN.hashCode();
        int idx = hash & (varName.length) - 1;
        while(true){
            String k = varName[idx];
            if(k == null) return null;
            if(hashes[idx] == hash && k.equals(varN)){
                return values[idx];
            }
            idx = (idx + 1) & (varName.length - 1);
        }
    }

    public boolean isDeclared(String varN){
        return get(varN) != null;
    }

    public int size(){
        return size;
    }

    private void resize(){
        VariablePool newPool = new VariablePool(varName.length * 2);
        for(int i = 0; i < varName.length; i++){
            if(varName[i] != null) newPool.assign(varName[i], values[i]);
        }
        this.varName = newPool.varName;
        this.values = newPool.values;
        this.hashes = newPool.hashes;
        this.threshold = newPool.threshold;
    }


}
