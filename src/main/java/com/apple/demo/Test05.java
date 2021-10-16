package com.apple.demo;

import org.apache.hadoop.hbase.util.Bytes;
import scala.reflect.io.Streamable;

import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.TreeSet;

/**
 * @Program: spark-java
 * @ClassName: Test05
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-09-28 12:03
 * @Version 1.1.0
 **/
public class Test05 {
    public static void main(String[] args) {
        int regions=3;
        String [] keys=new String[regions];
        DecimalFormat df=new DecimalFormat("00");

        for(int i=0;i<regions;i++){
            keys[i]=df.format(i)+"|";
        }
        byte[][] splitKeys=new byte[regions][];
        TreeSet<byte[]> treeSet=new TreeSet<>(Bytes.BYTES_COMPARATOR);




        System.out.println(Arrays.toString(keys));
    }
}
