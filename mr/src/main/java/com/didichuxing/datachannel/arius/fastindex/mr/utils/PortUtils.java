package com.didichuxing.datachannel.arius.fastindex.mr.utils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PortUtils {

    // 检测端口冲突
    public static List<Integer> choicePort() throws Exception {
        List<Integer> ports = new ArrayList<>();
        for(int i=9200; i<10200; i++) {
            ports.add(i);
        }


        Set<Integer> getPort = new HashSet<>();
        Integer port = choiceOnePort(ports, getPort);
        if(port<0) {
            throw new Exception("can not get port for es to use");
        }

        getPort.add(port);

        port = choiceOnePort(ports, getPort);
        if(port<0) {
            throw new Exception("can not get port for es to use");
        }

        getPort.add(port);



        List<Integer> ret = new ArrayList<>();
        for(Integer p : getPort) {
            ret.add(p);
        }
        return ret;
    }


    private static int choiceOnePort(List<Integer> pool, Set<Integer> exclude) {
        for(int i=0; i<1000; i++) {
            Integer index = (int) (Math.random() * pool.size());

            Integer port = pool.get(index);
            if(exclude.contains(port)) {
                continue;
            }

            if(!isLocalPortUsing(port)) {
                return port;
            }
        }

        return -1;
    }



    private static boolean isLocalPortUsing(int port){
        return isPortUsing("127.0.0.1", port);
    }

    private static boolean isPortUsing(String host,int port) {
        try {
            InetAddress Address = InetAddress.getByName(host);
            Socket socket = new Socket(Address,port);  //建立一个Socket连接
            if(socket.isConnected()) {
                socket.close();
                return true;
            }
            return false;
        } catch (IOException e) {
            return false;
        }
    }
}
