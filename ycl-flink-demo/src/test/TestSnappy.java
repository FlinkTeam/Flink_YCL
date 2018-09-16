import org.apache.hadoop.io.compress.SnappyCodec;
import org.junit.Test;

public class TestSnappy {

    @Test
    public void testNativeSnappy() {
        SnappyCodec.checkNativeCodeLoaded();
    }
}
