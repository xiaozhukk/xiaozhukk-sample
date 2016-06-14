package com.xiaozhukk.redis.action;

import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

import java.util.Random;
import java.util.Set;

/**
 * Created by admin on 2016/6/14.
 */
public class RankerTest {

    Jedis conn;


    static String INIT = "test:init";
    static String RANK = "test:rank";
    static String TEMP = "test:temp";
    static String TIME = "test:time";
    static String USER = "test:user";

    @Before
    public void setUp() {
        conn = new Jedis("localhost");
        conn.select(0);
    }

    @Test
    public void remove() {
        _remove();
    }

    @Test
    public void insertTest() throws InterruptedException {
        String ling = "98833";
        String yang = "99864";
        compare(ling, yang);


        insert(ling);
        Thread.sleep(10);
        insert(yang);
        compare(ling, yang);

        insert(yang);
        Thread.sleep(10);
        insert(ling);
        compare(ling, yang);

    }

    private void compare(String ling, String yang) {
        System.out.println("--- { ling } = " + conn.zrevrank(RANK, ling) + " [" + "RankerTest.insertTest]");
        System.out.println("--- { yang } = " + conn.zrevrank(RANK, yang) + " [" + "RankerTest.insertTest]");
    }


    @Test
    public void test() {
        init();

        Set<Tuple> result = conn.zrangeWithScores(RANK, 0, -1);

        getRank("1234");

        System.out.println("--- { result } = " + result.size() + " [" + "RankerTest.test]");
    }

    private void getRank(String user) {
        System.out.println(user + " : " + conn.zrevrank(RANK, user));
    }

    private void init() {

        if ("1".equals(conn.get(INIT))) {
            return;
        }

        Random ra = new Random();
        for (int i = 0; i < 100000; i++) {
            addUser(i + "", timestamp(ra.nextInt(10000)), ra.nextInt(1000));
        }

        conn.zunionstore(RANK, TEMP, TIME);

        conn.set(INIT, "1");

        // String user1 = "001";
        // addUser(user1, timestamp(), 5);
        //
        //
        // String user2 = "002";
        // addUser(user2, timestamp(), 4);
        //
        // String user3 = "003";
        // addUser(user3, timestamp(-100), 4);
        //
        // String user4 = "004";
        // addUser(user4, timestamp(), 6);
    }

    private void insert(String user) {
        conn.zadd(TIME, timestamp(0), user);
        conn.zincrby(TEMP, 1, user);
        conn.zadd(RANK, conn.zscore(TEMP, user) + conn.zscore(TIME, user), user);
    }

    private void addUser(String user, double timestamp, int times) {
        // conn.zadd(RANK, 1, user);
        // conn.zadd(TEMP, 1, user);
        conn.zadd(TIME, timestamp, user);
        conn.zadd(TEMP, times, user);
        // increase(1, user, times);
    }

    public void increase(double value, String user, Integer times) {
        for (int i = 0; i < times; i++) {
            conn.zincrby(TEMP, value, user);
        }
    }

    // private void update(String user) {
    //     conn.zincrby(TEMP, 1, user);
    //     conn.zadd(TIME, timestamp(), user);
    //     conn.zadd(RANK, conn.zscore(TEMP,user) + conn.zscore(TIME,user), user);
    // }

    public double timestamp() {
        return timestamp(0);
    }

    public double timestamp(long time) {
        return (System.currentTimeMillis() - time) / 10000000000000D;
    }


    public void _remove() {
        conn.del(RANK);
        conn.del(TIME);
        conn.del(TEMP);
        conn.del(USER);
    }


}