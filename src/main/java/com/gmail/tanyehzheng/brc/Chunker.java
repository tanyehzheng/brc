package com.gmail.tanyehzheng.brc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.charset.StandardCharsets;

public class Chunker {
    private final FileChannel fc;
    private final long fileSize;
    private long from = 0;

    public Chunker(FileChannel fc) throws IOException {
        this.fc = fc;
        this.fileSize = fc.size();
    }

    public boolean hasNext() {
        return from < fileSize;
    }

    public MappedByteBuffer nextChunk(){
        try {
            long to = getPositionOfNewLineFrom(fc, Math.min(fileSize-1, from + Constants.CHUNK_SIZE));
            MappedByteBuffer mbb = fc.map(MapMode.READ_ONLY, from, to - from);
            from = to;
            return mbb;
        } catch (IOException e) {
            e.printStackTrace();
        }
        throw new IllegalStateException("Shouldn't end up here");
    }

    public void printAtPosition(FileChannel ch, long position) throws IOException {
        ByteBuffer bb = ByteBuffer.allocate(Constants.SMALL_BUFFER_SIZE);
        bb.clear();
        ch.read(bb, position);
        bb.flip();
        System.out.println(StandardCharsets.UTF_8.decode(bb));
    }

    public long getPositionOfNewLineFromMidpoint(FileChannel ch) throws IOException {
        final long size = ch.size();
        long position = size / 2;
        return getPositionOfNewLineFrom(ch, position);
    }

    public long getPositionOfNewLineFrom(FileChannel ch, long position) throws IOException {
        ByteBuffer bb = ByteBuffer.allocate(Constants.SMALL_BUFFER_SIZE);
        ch.read(bb, position);
        bb.flip();
        while (bb.get() != '\n')
            position++;
        position++;
        return position;
    }
}
