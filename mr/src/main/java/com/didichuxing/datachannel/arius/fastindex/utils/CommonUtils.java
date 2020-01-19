package com.didichuxing.datachannel.arius.fastindex.utils;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

@Slf4j
public class CommonUtils {
    public static void tarFile(String srdDir, String dstTar) {
        String duSrcCmd = "du -sh " + srdDir;
        try {
            execCmd(duSrcCmd);
        } catch (Exception e) {
            log.error("cmd:{},execute error!", duSrcCmd, e);
        }

        //对es数据tar打包,最多重试3次
        log.info("start tar indexDir:{}...", srdDir);
        String tarCmd = "tar -cvf " + dstTar + " " + srdDir;
        int tryCount = 3;
        for (int i = 0; i < tryCount; i++) {
            try {
                execCmd(tarCmd);
                break;
            } catch (Throwable t) {
                log.error("tar index error!try:{}", i, t);
                String delCmd = "rm -rf " + dstTar;
                try {
                    execCmd(delCmd);
                } catch (Throwable t1) {
                    log.error("cmd:{},execute error!", delCmd, t1);
                }
            }
        }
        log.info("tar indexDir:{} finished!", srdDir);

        String duDstCmd = "du -sh " + dstTar;
        try {
            execCmd(duDstCmd);
        } catch (Throwable t) {
            log.error("cmd:{},execute error!", duDstCmd, t);
        }
    }




    public static void execCmd(String cmd) throws Exception {
        String[] cmds = {"/bin/sh", "-c", cmd};
        Process pro = Runtime.getRuntime().exec(cmds);
        pro.waitFor();
        InputStream in = pro.getInputStream();
        BufferedReader read = new BufferedReader(new InputStreamReader(in));
        String line;
        while ((line = read.readLine()) != null) {
            LogUtils.info("cmd:" + cmd + ",result:" + line);
        }
    }

    /* 基于es 2.0计算docId所属分片 */
    public static int getShardId(String docId, int shardsNum) {
        int hash = murmur3hash(docId);
        return mod(hash, shardsNum);
    }

    public static int mod(int v, int m) {
        int r = v % m;
        if (r < 0) {
            r += m;
        }
        return r;
    }

    private static int murmur3hash(String routing) {
        final byte[] bytesToHash = new byte[routing.length() * 2];
        for (int i = 0; i < routing.length(); ++i) {
            final char c = routing.charAt(i);
            final byte b1 = (byte) c, b2 = (byte) (c >>> 8);
            assert ((b1 & 0xFF) | ((b2 & 0xFF) << 8)) == c; // no information loss
            bytesToHash[i * 2] = b1;
            bytesToHash[i * 2 + 1] = b2;
        }
        return murmur3hash(bytesToHash, 0, bytesToHash.length);
    }

    private static int murmur3hash(byte[] bytes, int offset, int length) {
        return murmurhash3_x86_32(bytes, offset, length, 0);
    }

    public static int murmurhash3_x86_32(byte[] data, int offset, int len, int seed) {

        final int c1 = 0xcc9e2d51;
        final int c2 = 0x1b873593;

        int h1 = seed;
        int roundedEnd = offset + (len & 0xfffffffc);  // round down to 4 byte block

        for (int i=offset; i<roundedEnd; i+=4) {
            // little endian load order
            int k1 = (data[i] & 0xff) | ((data[i+1] & 0xff) << 8) | ((data[i+2] & 0xff) << 16) | (data[i+3] << 24);
            k1 *= c1;
            k1 = Integer.rotateLeft(k1, 15);
            k1 *= c2;

            h1 ^= k1;
            h1 = Integer.rotateLeft(h1, 13);
            h1 = h1*5+0xe6546b64;
        }

        // tail
        int k1 = 0;

        switch(len & 0x03) {
            case 3:
                k1 = (data[roundedEnd + 2] & 0xff) << 16;
                // fallthrough
            case 2:
                k1 |= (data[roundedEnd + 1] & 0xff) << 8;
                // fallthrough
            case 1:
                k1 |= (data[roundedEnd] & 0xff);
                k1 *= c1;
                k1 = Integer.rotateLeft(k1, 15);
                k1 *= c2;
                h1 ^= k1;
        }

        // finalization
        h1 ^= len;

        // fmix(h1);
        h1 ^= h1 >>> 16;
        h1 *= 0x85ebca6b;
        h1 ^= h1 >>> 13;
        h1 *= 0xc2b2ae35;
        h1 ^= h1 >>> 16;

        return h1;
    }
}
