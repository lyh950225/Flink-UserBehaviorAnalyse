import redis.clients.jedis.Jedis;

/**
 * @author 林叶辉
 * @version V1.0
 * @Package PACKAGE_NAME
 * @date 2020/10/24 22:26
 */
public class Redis_Test {
    public static void main(String[] args) throws InterruptedException {
        Jedis jedis = new Jedis("192.168.154.201", 6379);
        String pong = jedis.ping();
        System.out.println("response: " + pong);

        //key-value类型写入
//        jedis.set("k1","v1");
//        jedis.set("k2","v2");
//        jedis.set("k3","v3");
        //如果key不存在就添加、key的有效时间
//        jedis.set("k4","v4","NX","EX",10);

        //返回k3的值，并重新设置为v33
//        String re = jedis.getSet("k3", "v33");
        //判断某个key是否存在
//        Boolean exists = jedis.exists("k3");
//        System.out.println(exists);
        //ttl key 查看还有多少秒过期，-1表示永不过期，-2表示已过期
//        Long k3 = jedis.ttl("k3");
//        System.out.println(k3);
        // type key 查看你的key是什么类型
//        String type = jedis.type("mylist");
//        System.out.println(type);

        //类似subString,但是getrange的offset范围是全闭合区间
//        String getrange = jedis.getrange("k3", 0, 2);
//        System.out.println(getrange);

        //重指定的下标开始（包括该下标），用value的值依次往后填充
//        jedis.setrange("key3",1,"22");
//        String key3 = jedis.get("key3");
//        System.out.println(key3);

        //删除key
//        jedis.del("key3");
//        String key3 = jedis.get("key3");
//        System.out.println(key3);

        //设置一个键值对，并赋予TTL
//        jedis.setex("key4",10,"v4");
//        Thread.sleep(1000L);
//        System.out.println(jedis.ttl("key4"));  //9s

        //如果不存在，则设置一个键值对
//        System.out.println(jedis.setnx("key4", "v44")); //假设已存在,返回0,表示失败


    }
}
