package test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

/**
 * @author 林叶辉
 * @version V1.0
 * @Package test
 * @date 2020/12/15 22:00
 */
public class leetcode {
    public static void main(String[] args) {
        int[] ints = new int[]{1,3,2,5,4};
        Arrays.sort(ints);
        System.out.println(ints);

        findRepeatNumber("We are happy.");
    }
    public static void findRepeatNumber(String s) {
        String replace = s.replace(" ", "%20");
        System.out.println(replace);
    }
}
