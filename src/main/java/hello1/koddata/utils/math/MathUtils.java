package hello1.koddata.utils.math;

public class MathUtils {

    public static int nearestPowerOf2(int n){
        if (n < 1){
            return 1;
        }

        int k = (int) Math.log(n);
        int lower = (int) Math.pow(2, k);
        int upper = (int) Math.pow(2, k + 1);

        return n - lower < upper ? lower : upper;
    }

}
