import org.junit.Test;
import org.springframework.jdbc.support.incrementer.SybaseAnywhereMaxValueIncrementer;

/**
 * Created by jianghui on 18-8-27.
 */
public class TestEnv {

    @Test
    public void getEnv() {
        System.out.println(System.getenv("HADOOP_HOME"));
    }
}
