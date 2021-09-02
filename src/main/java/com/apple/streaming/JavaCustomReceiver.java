package com.apple.streaming;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.Socket;

/**
 * @Program: spark-java
 * @ClassName: JavaCustomReceiver
 * @Description: 自定义的receiver
 * @Author Mr.Apple
 * @Create: 2021-09-01 22:42
 * @Version 1.1.0
 * 某个案例：https://www.cnblogs.com/ChouYarn/p/7992724.html
 **/
public class JavaCustomReceiver extends Receiver<String> {

    String host = null;
    int port = -1;

    public JavaCustomReceiver(String host_, int port_) {
        super(StorageLevel.MEMORY_AND_DISK_2());
        host = host_;
        port = port_;
    }


    @Override
    public void onStart() {
        new Thread() {
            @Override
            public void run() {
                super.run();
                receive();
            }
        }.start();
    }

    private void receive() {
        Socket socket = null;
        String userInput = null;

        try {
            // connect to the server
            socket = new Socket(host, port);

            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            // Until stopped or connection broken continue reading
            while (!isStopped() && (userInput = reader.readLine()) != null) {
                System.out.println("Received data '" + userInput + "'");
                store(userInput);
            }
            reader.close();
            socket.close();

            // Restart in an attempt to connect again when server is active again
            restart("Trying to connect again");
        } catch (ConnectException ce) {
            // restart if could not connect to server
            restart("Could not connect", ce);
        } catch (Throwable t) {
            // restart if there is any other error
            restart("Error receiving data", t);
        }
    }

    @Override
    public void onStop() {

    }
}
