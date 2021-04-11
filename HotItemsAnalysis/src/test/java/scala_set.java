import redis.clients.jedis.Jedis;

/**
 * @author 林叶辉
 * @version V1.0
 * @Package PACKAGE_NAME
 * @date 2020/10/25 1:47
 */
public class scala_set {
    public static void main(String[] args) {
        Jedis jedis = new Jedis("192.168.154.201", 6379);

        //redis中对set的操作往往以s开头  set呢? 滚啊
//        jedis.sadd("Set","0","0","1","2","3");
        //smembers:返回该key对应的set的全部值
//        System.out.println(jedis.smembers("Set"));
        //sismember:set中是否有对应的值
//        System.out.println(jedis.sismember("Set", "0"));
        //scard:返回set中的元素个数
//        System.out.println(jedis.scard("Set"));
        //srem:remove集合中的某个value
//        jedis.srem("Set","0");
//        System.out.println(jedis.smembers("Set"));
        //srandmember：随机弹出set中的几个元素
//        System.out.println(jedis.srandmember("Set", 2));

        //数学上的交集、差集、并集
//        jedis.sinter("Set1","Set2");
//        jedis.sdiff("Set1","Set2");
//        jedis.sunion("Set1","Set2");
    }
}
