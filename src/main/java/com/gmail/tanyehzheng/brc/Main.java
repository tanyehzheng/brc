package com.gmail.tanyehzheng.brc;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.TreeMap;

public class Main {
    static final Path file = Path.of("/", "Users", "yehzheng", "code", "1brc", "measurements.txt");
    public static void main(String[] args) {
        new Parser(file).parse();
    }

    public static void main1(String[] args) throws IOException {
        try (final FileChannel ch = FileChannel.open(file, StandardOpenOption.READ)) {
            // readFileWithByteBuffer(ch);
            readFileWithMappedByteBuffer(ch);

            // printAtPosition(ch, position);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        System.out.println("end");
    }



    private static void readFileWithByteBuffer(FileChannel ch) throws IOException {
        ByteBuffer bb = ByteBuffer.allocate(Constants.LARGE_BUFFER_SIZE);
        while (ch.read(bb) != -1) {
            bb.flip();
            bb.clear();
        }

    }

    static CharsetDecoder utf8Decoder = StandardCharsets.UTF_8.newDecoder();
    static CharsetDecoder asciiDecoder = StandardCharsets.US_ASCII.newDecoder();
    private static void readFileWithMappedByteBuffer(FileChannel ch) throws IOException {
        final long fileSize = ch.size();
        long bytesRead = 0;
        final long oriBufferSize = Constants.LARGE_BUFFER_SIZE;
        long bufferSize = oriBufferSize;
        // CharBuffer mbb = CharBuffer.allocate((int) bufferSize);
        while (bytesRead < fileSize) {
            long remainingBytes = fileSize - bytesRead;
            bufferSize = Math.min(bufferSize, remainingBytes);

            MappedByteBuffer bb = ch.map(MapMode.READ_ONLY, bytesRead, bufferSize);
            // codec.decode(bb, mbb, remainingBytes == 0);
            bytesRead += bufferSize;
            // mbb.flip();
            // System.out.print(".");

            process(bb);
            // mbb.clear();
            // break;
        }

        map.entrySet().stream().forEach(e -> System.out.printf("City: %s, %s\n", e.getKey(), e.getValue()));
    }

    static Map<String, Stats> map = new TreeMap<>();
    static StringBuilder sb = new StringBuilder();
    static String city = "";
    static ByteBuffer buffer = ByteBuffer.allocate(Constants.SMALL_BUFFER_SIZE);

    private static void process(MappedByteBuffer mbb) throws CharacterCodingException {
        while(mbb.hasRemaining()) {
            byte c = mbb.get();
            switch(c) {
                case ';':
                    city = utf8Decoder.decode(buffer.flip()).toString();
                    buffer.clear();
                    break;
                case '\n':
                    String reading = asciiDecoder.decode(buffer.flip()).toString();
                    buffer.clear();
                    Stats stats = map.computeIfAbsent(city, k -> new Stats());
                    stats.addReading(Double.parseDouble(reading));
                    break;
                default:
                    buffer.put(c);
            }
        }
    }

    private static void process(CharBuffer mbb) {
        while (mbb.hasRemaining()) {
            char c = mbb.get();
            // System.out.print(c);
            switch (c) {
                case ';':
                    city = sb.toString();
                    sb.delete(0, sb.length());
                    break;
                case '\n':
                    Stats stats = map.computeIfAbsent(city, k -> new Stats());
                    stats.addReading(Double.parseDouble(sb.toString()));
                    sb.delete(0, sb.length());
                    break;
                default:
                    sb.append(c);
                    break;
            }
        }
    }

    private static void process1(CharBuffer mbb) {
        int prev = 0;
        String city = "";
        String reading = "";
        while (mbb.hasRemaining()) {
            switch (mbb.get()) {
                case ';':
                    CharBuffer slice = mbb.slice(prev, mbb.position() - prev - 1);
                    // print(slice);
                    city = slice.toString();
                    // System.out.println("City: " + city);
                    prev = mbb.position();
                    break;
                case '\n':
                    if (city.isEmpty()) {
                        continue;
                    }

                    CharBuffer slice1 = mbb.slice(prev, mbb.position() - prev - 1);
                    // print(slice);
                    reading = slice1.toString();
                    System.out.println("city: " + city + ", reading: " + reading);
                    prev = mbb.position();

                    Stats stats = map.computeIfAbsent(city, k -> new Stats());
                    stats.addReading(Double.parseDouble(reading));
                    break;

                default:
            }
        }
    }

    private static void print(CharBuffer slice) {
        System.out.printf("Position: %d, Limit: %d, Capacity: %d\n", slice.position(), slice.limit(), slice.capacity());
    }

    private static void readFileWithBufferedReader(Path file) throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()), 2_048_000)) {
            String line;
            int count = 1000_000_00;
            while ((line = reader.readLine()) != null) {
                count--;
                if (count < 0) {
                    break;
                }
            }
            System.out.println(line);
        }
    }
}