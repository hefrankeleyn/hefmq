package io.github.hefrankeleyn.hefmq.store;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.github.hefrankeleyn.hefmq.model.HefMessage;
import org.junit.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.Scanner;

/**
 * @Date 2024/7/30
 * @Author lifei
 */
public class StoreTest {

    private static final String topic = "test";

    @Test
    public void test01() {
        try {
            // 创建文本
            String content = "This is a file.\n This is a new line for store.\n";
            // 创建文件
            File file = new File("test.dat");
            Map<Integer, Integer> indexOffsetMap = Maps.newHashMap();
            if (!file.exists()) {
                boolean newFile = file.createNewFile();
                System.out.println("===> 创建一个新文件： " + newFile);
            }
            // 定义一个Path
            Path path = Paths.get(file.getAbsolutePath());
            // 获取FileChannel
            try (FileChannel fileChannel = (FileChannel) Files.newByteChannel(path, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, 1024 * 1024);
                for (int i = 0; i < 10; i++) {
                    String body = Strings.lenientFormat("%s : %s", i, content);
                    HefMessage<String> hefMessage = HefMessage.createHefMessage(body);
                    byte[] bytes = new Gson().toJson(hefMessage).getBytes(StandardCharsets.UTF_8);
                    int len = bytes.length;
                    System.out.println(Strings.lenientFormat("====> %s, offset: %s, len: %s", i, mappedByteBuffer.position(), len));
                    Indexer.addEntry(topic, mappedByteBuffer.position(), len);
                    indexOffsetMap.put(i, mappedByteBuffer.position());
                    mappedByteBuffer.put(bytes);
                }

                // 创建一个只读的buffer
                ByteBuffer readOnlyBuffer = mappedByteBuffer.asReadOnlyBuffer();
                Scanner scanner = new Scanner(System.in);
                while (scanner.hasNext()) {
                    String line = scanner.nextLine();
                    if (line.equals("q")) {
                        break;
                    }
                    System.out.println("===> scanner IN: " + line);
                    int index = Integer.parseInt(line);
                    if(!indexOffsetMap.containsKey(index)){
                        continue;
                    }
                    Integer offset = indexOffsetMap.get(index);
                    Indexer.Entry entry = Indexer.getEntry(topic, offset);
                    readOnlyBuffer.position(offset);
                    byte[] resBytes = new byte[entry.getLen()];
                    readOnlyBuffer.get(resBytes);
                    String res = new String(resBytes, StandardCharsets.UTF_8);
                    TypeToken<HefMessage<String>> typeToken = new TypeToken<>() {};
                    HefMessage<String> resMessage = new Gson().fromJson(res, typeToken.getType());
                    System.out.println("===> read result: " + resMessage);
                }
            }
        }catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void test02() {
        try {
            // 创建文本
            String content = "This is a file.\n This is a new line for store.\n";
            Map<Integer, Integer> mapLen = Maps.newHashMap();
            // 创建文件
            File file = new File("test02.dat");
            if (!file.exists()) {
                boolean newFile = file.createNewFile();
                System.out.println("===> 创建一个新文件： " + newFile);
            }
            // 获取FileChannel
            try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
                 FileChannel fileChannel = randomAccessFile.getChannel()) {
                MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, 1024 * 1024);
                for (int i = 0; i < 10; i++) {
                    byte[] bytes = Strings.lenientFormat("%s : %s", i, content).getBytes(StandardCharsets.UTF_8);
                    int len = bytes.length;
                    mapLen.put(i, bytes.length);
                    System.out.println(Strings.lenientFormat("====> %s, offset: %s, len: %s", i, mappedByteBuffer.position(), len));
                    mappedByteBuffer.put(bytes);
                }

                // 创建一个只读的buffer
                ByteBuffer readOnlyBuffer = mappedByteBuffer.asReadOnlyBuffer();
                Scanner scanner = new Scanner(System.in);
                while (scanner.hasNext()) {
                    String line = scanner.nextLine();
                    if (line.equals("q")) {
                        break;
                    }
                    System.out.println("===> scanner IN: " + line);
                    int index = Integer.parseInt(line);
                    if(!mapLen.containsKey(index)){
                        continue;
                    }
                    int len = mapLen.get(index);
                    int offset = len * index;
                    readOnlyBuffer.position(offset);
                    byte[] resBytes = new byte[len];
                    readOnlyBuffer.get(resBytes);
                    System.out.println("===> read result: " + new String(resBytes, StandardCharsets.UTF_8));
                }
            }
        }catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void test03() {
        String res = String.format("%010d", 10);
        String res02 = Strings.padStart(String.valueOf(10), 10, '0');
        System.out.println(res);
        System.out.println(res02);
        System.out.println(res.equals(res02));
    }

    @Test
    public void test04() {
        String message = "0000000102";
        boolean matches = message.matches("^[0-9]+$");
        System.out.println(matches);
    }
}
