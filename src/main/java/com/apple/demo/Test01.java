package com.apple.demo;

import java.util.concurrent.locks.LockSupport;

/**
 * @Program: spark-java
 * @ClassName: Test01
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-08-22 04:09
 * @Version 1.1.0
 **/
public class Test01 {
    public static Object u = new Object();
    static ChangeObjectThread t1 = new ChangeObjectThread("t1");
    static ChangeObjectThread t2 = new ChangeObjectThread("t2");

    public static void main(String[] args) throws InterruptedException {
        t1.start();
        Thread.sleep(1000L);
        t2.start();
        Thread.sleep(3000L);
        t1.interrupt();
        LockSupport.unpark(t2);
        t1.join();
        t2.join();
    }

    public static class ChangeObjectThread extends Thread {
        public ChangeObjectThread(String name) {
            super(name);
        }

        @Override
        public void run() {
            synchronized (u) {
                System.out.println("in " + getName());
                LockSupport.park();
                if (Thread.currentThread().isInterrupted()) {
                    System.out.println("被中断了 " + getName());
                }
                System.out.println("继续执行 " + getName());
            }
        }
    }
}
