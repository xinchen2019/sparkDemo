package com.apple.demo;

/**
 * @Program: spark-java
 * @ClassName: Run
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-08-23 09:45
 * @Version 1.1.0
 **/
public class Run {
    public static void main(String[] args) {
        Service service = new Service();
        ThreadA threadA = new ThreadA(service);
        threadA.start();
        service.getUsernamePassword();
        ThreadB threadB = new ThreadB(service);
        threadB.start();
        service.getUsernamePassword();
    }
}

class Service {
    String anything = new String();
    private String usernameParam;
    private String passwordParam;

    public void setUsernamePassword(String username, String password) {
        try {
            synchronized (anything) {
                System.out.println("线程名称为：" + Thread.currentThread().getName() + "在" + System.currentTimeMillis() + "进入同步块");
                usernameParam = username;
                Thread.sleep(3000);
                passwordParam = password;
                System.out.println("线程名称为：" + Thread.currentThread().getName() + "在" + System.currentTimeMillis() + "离开同步块");
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void getUsernamePassword() {
        System.out.println("==============begin getUsernamePassword=============");
        System.out.println("线程名称为：" + Thread.currentThread().getName());
        System.out.println("usernameParam " + usernameParam);
        System.out.println("passwordParam " + passwordParam);
        System.out.println("==============end   getUsernamePassword=============");
    }
}

class ThreadA extends Thread {
    private Service service;

    public ThreadA(Service service) {
        super();
        this.service = service;
    }

    @Override
    public void run() {
        super.run();
        service.setUsernamePassword("aa", "aapwd");
        service.getUsernamePassword();
    }
}

class ThreadB extends Thread {
    private Service service;

    ThreadB(Service service) {
        super();
        this.service = service;
    }

    @Override
    public void run() {
        super.run();
        service.setUsernamePassword("bb", "bbpwd");
        service.getUsernamePassword();
    }
}