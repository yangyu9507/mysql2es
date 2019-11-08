import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.collection.SynchronizedCollection;
import org.apache.commons.collections4.queue.SynchronizedQueue;
import org.apache.commons.lang3.StringUtils;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

/**
 * created by yangyu on 2019-10-28
 */
public class NumberTest {

    private static final List<String> PHONE_NUMBER_SPLITS
            = Arrays.asList("+","-");


    @Test
    public void test1() {
        String num1 = "176*****971";
        System.out.println(trans(num1));
        System.out.println(trans("948*****x68"));
        System.out.println(trans("abcx68fasfsaddsfh"));
        System.out.println(trans("021-1234455"));
        System.out.println(trans("081 9011 00077"));
        System.out.println(trans("+8615498523156"));
    }


    private boolean trans(String phoneNumber) {
        if (StringUtils.isBlank(phoneNumber)){
            return false;
        }
        phoneNumber = phoneNumber.replaceAll("\\d","");
        for (String phoneNumberSplit : PHONE_NUMBER_SPLITS){
            phoneNumber = StringUtils.remove(phoneNumber,phoneNumberSplit);
        }

        return StringUtils.isNotBlank(phoneNumber);
    }

}

