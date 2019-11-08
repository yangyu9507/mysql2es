import gy.lib.common.util.FinanceUtil;
import gy.lib.common.util.NumberUtil;
import org.apache.commons.lang3.StringUtils;

import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * created by yangyu on 2019-09-19
 */

class Num{
    public Num(int a) {
        this.a = a;
    }

    int a;


    public int getA() {
        return a;
    }

    public void setA(int a) {
        this.a = a;
    }
}

public class Test {

    public static void main(String...args) throws Exception{
        /*long start = System.currentTimeMillis();
        long end = start + 225859;
        Double totalTime = FinanceUtil.divide(NumberUtil.toDouble(end - start), NumberUtil.toDouble(1000));
        int min  = NumberUtil.toInt(Math.floor(totalTime / 60));
        String sec  = String.format("%.3f",totalTime % 60);
        System.out.println(String.format("%dm %ss",min,sec));*/


       // String ab=String.format("%.2f",1.23);
        //System.out.println(ab);
        /*boolean at=false ;
        boolean ab= true ;
        System.out.println("assert ="+!(at && ab));
        System.out.println("assert ="+!(at || ab));*/

    }

    private static AtomicBoolean isCreated = new AtomicBoolean(false);

    @org.testng.annotations.Test
    public void tttt() {

        if (!isCreated.get()){
            System.out.println("create .. ");
            isCreated.set(true);
        }
        System.out.println(isCreated.get());

    }





    @org.testng.annotations.Test
    public void test2() {
        System.out.println(0 % 100000 == 0);
    }

    @org.testng.annotations.Test
    public void test3() {
        System.out.println(StringUtils.substringBefore("sku_","_"));
    }

    @org.testng.annotations.Test
    public void test4() throws Exception {
        System.out.println(1570869464158L + 24 * 60 *  60 * 1000);
    }

}
