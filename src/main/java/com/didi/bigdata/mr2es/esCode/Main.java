package com.didi.bigdata.mr2es.esCode;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Paths;

/**
 * es方代码
 */
public class Main {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.err.println("Usage: " +
                    "IndexMergeTool <mergedIndex> [index] ...");
            System.exit(1);
        }

        if(args.length==1) {
            System.out.println("Single Index...");
            return;
        }

        //mergedIndex: 待merge的index文件
        Directory mergedIndex = FSDirectory.open(Paths.get(args[0]));

        IndexWriter writer = new IndexWriter(mergedIndex,
                new IndexWriterConfig(null)
                        .setOpenMode(IndexWriterConfig.OpenMode.APPEND));

        //indexs: 等待被merge的index文件路径
        Directory[] indexes = new Directory[args.length - 1];
        for (int i = 1; i < args.length; i++) {
            indexes[i - 1] = FSDirectory.open(Paths.get(args[i]));
        }

        writer.addIndexes(indexes);

        writer.close();
        System.out.println("Done.");
    }
}