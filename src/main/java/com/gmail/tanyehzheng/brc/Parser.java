package com.gmail.tanyehzheng.brc;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class Parser {

    private final Path file;

    public Parser(Path path) {
        this.file = path;
    }

    public void parse() {
        try (
            final FileChannel ch = FileChannel.open(file, StandardOpenOption.READ); 
            final ExecutorService exe = Executors.newVirtualThreadPerTaskExecutor();
        ) {
            Chunker chunker = new Chunker(ch);
            ExecutorCompletionService<Map<String, Stats>> service = new ExecutorCompletionService<>(exe);
            int count = 0;
            while(chunker.hasNext()){
                count++;
                MappedByteBuffer chunk = chunker.nextChunk().load();

                Processor p = new Processor(chunk);
                Future<Map<String, Stats>> f = service.submit(p);
                // f.get();
                System.out.println("processed lines: " + p.getLinesProcessed());
                // break;
            }
            for (int i = 0; i < count; i++) {
                service.take();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
