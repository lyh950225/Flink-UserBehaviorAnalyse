import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;

/**
 * @author 林叶辉
 * @version V1.0
 * @Package PACKAGE_NAME
 * @date 2020/10/25 1:58
 */
public class redis_hash {
    public static void main(String[] args) {
        Jedis jedis = new Jedis("192.168.154.201", 6379);

        //Hash的key是一个String,value可以看作Java中的map<String,String>
//        jedis.hset("Hash","k1","v1");
//        System.out.println(jedis.hget("Hash", "k1"));
        HashMap<String, String> map = new HashMap<>();
        map.put("k2","v2");
        map.put("k3","v3");
        jedis.hmset("Hash",map);
        System.out.println(jedis.hmget("Hash", "k1", "k2", "k3"));

        System.out.println(jedis.hkeys("Hash"));
        System.out.println(jedis.hvals("Hash"));
    }
}
