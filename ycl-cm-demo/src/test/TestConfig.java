import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.io.File;
import java.util.List;

public class TestConfig {

    @Test
    public void testClientConnect() {
        Jedis jedis = new Jedis("192.168.251.73", 6379);
        System.out.println(jedis.get("a"));
        jedis.close();
    }

    @Test
    public void testFilePath() {
        String dataMappingPath = "config\\datamap";
        File baseFile = new File(dataMappingPath);
        File[] childFlies = baseFile.listFiles();
       // childFlies = null;
        for (File file : childFlies) {
            System.out.println(file.getName());
            System.out.println(file.getPath());
        }
    }

}
