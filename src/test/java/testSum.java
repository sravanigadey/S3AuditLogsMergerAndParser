import com.logs.Sum;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class testSum {

    Sum sum = new Sum();
    int sumResult = sum.add(2, 3);

    @Test
    public void sumTest() {
//        assertEquals(50, addi);
        assertEquals(5, sumResult);
    }
}
