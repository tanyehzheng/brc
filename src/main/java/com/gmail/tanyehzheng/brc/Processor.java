package com.gmail.tanyehzheng.brc;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;

public class Processor implements Callable<Map<String, Stats>> {

    private final MappedByteBuffer mbb;
    private final ByteBuffer buffer = ByteBuffer.allocate(Constants.SMALL_BUFFER_SIZE);
    private final CharsetDecoder utf8Decoder = StandardCharsets.UTF_8.newDecoder();
    private final CharsetDecoder asciiDecoder = StandardCharsets.US_ASCII.newDecoder();
    private String city;
    private Map<String, Stats> map = new TreeMap<>();
    private int lines = 0;

    public Processor(MappedByteBuffer chunk) {
        this.mbb = chunk;
    }

    public int getLinesProcessed() {
        return this.lines;
    }

    public Map<String, Stats> call() {
        int index = 0;
        int length = 0;
        MappedByteBuffer slice = null;
        try {
            while (mbb.hasRemaining()) {
                byte c = mbb.get();
                switch (c) {
                    case ';':
                        slice = mbb.slice(index, length);
                        index = mbb.position();
                        length = 0;
                        city = utf8Decoder.decode(slice).toString();// 5s
                        // city = new String(slice.array(), "UTF-8");
                        // System.out.println("city: "+city);
                        break;
                    case '\n':
                        lines++;
                        slice = mbb.slice(index, length);
                        index = mbb.position();
                        length = 0;
                        String reading = asciiDecoder.decode(slice).toString();// 2s
                        // System.out.println("reading: " + reading);
                        Stats stats = map.computeIfAbsent(city, k -> new Stats());
                        stats.addReading(Double.parseDouble(reading));
                        break;
                    default:
                        length++;
                }
            }
        } catch (CharacterCodingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return this.map;
    }

    public void process1() throws CharacterCodingException {
        while (mbb.hasRemaining()) {
            byte c = mbb.get();
            switch (c) {
                case ';':
                    buffer.flip();
                    city = utf8Decoder.decode(buffer).toString(); // 4s
                    buffer.clear();
                    break;
                case '\n':
                    buffer.flip();
                    String reading = asciiDecoder.decode(buffer).toString(); // 2.3s
                    buffer.clear();
                    Stats stats = map.computeIfAbsent(city, k -> new Stats());
                    stats.addReading(Double.parseDouble(reading));
                    break;
                default:
                    buffer.put(c);
            }
        }
    }

}
