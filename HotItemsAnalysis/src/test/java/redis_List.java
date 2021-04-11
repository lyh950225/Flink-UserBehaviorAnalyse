import redis.clients.jedis.Jedis;

/**
 * @author 林叶辉
 * @version V1.0
 * @Package PACKAGE_NAME
 * @date 2020/10/25 1:22
 */
public class redis_List {
    public static void main(String[] args) {
        Jedis jedis = new Jedis("192.168.154.201", 6379);

        //redis中的List操作往往带l开头
        //get set 针对String型的键
        //将对里面的数，一般都是push pop 或者指定index取数，底层是一个双向链表
        //如果值都取出来了，键也将消失

//        jedis.lpush("List0","0","1","2"); //2 1 0  左边添加，看作栈比较好理解，但是没有栈底
//        System.out.println(jedis.type("List0"));

        //双向链表所以可以从左边或者右边添加
        //本来是“List0”: 2 1 0
//        jedis.lpush("List0","-1");
//        jedis.rpush("List0","4");
//        System.out.println("index: " + jedis.lindex("List0", 0));   //-1
//        System.out.println(jedis.lrange("List0", 0, -1));

        //lrem:移除N个value
        // -1 -1 2 1 0 4
//        jedis.lpush("List0","-1");
//        jedis.lrem("List0",2,"-1");
        //2, 1, 0, 4
//        System.out.println(jedis.lrange("List0", 0, -1));

        //截取start->end位的value值，再重新赋值给这个key
//        String ltrim = jedis.ltrim("List0", 0, 1);
//        System.out.println(jedis.lrange("List0", 0, -1));

        //设置列表中某个index的值
//        jedis.lset("List0",0,"1");
//        System.out.println(jedis.lrange("List0",0,-1));


    }
}
