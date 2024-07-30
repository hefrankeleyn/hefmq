package io.github.hefrankeleyn.hefmq.store;

import static com.google.common.base.Preconditions.*;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.github.hefrankeleyn.hefmq.model.HefMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Objects;

/**
 * @Date 2024/7/30
 * @Author lifei
 */
public class HefStore {
    private static final Logger log = LoggerFactory.getLogger(HefStore.class);
    private final String topic;
    public static final Integer LEN = 1024 * 1024;
    private MappedByteBuffer mappedByteBuffer;
    private final Gson gson = new Gson();

    public HefStore(String topic) {
        this.topic = topic;
    }

    private File topicFile() {
        try {
            File file = new File(Strings.lenientFormat("%s.bat", topic));
            if (!file.exists()) {
                boolean newFile = file.createNewFile();
                log.info("====> create file {}, {}", file.getName(), newFile);
            }
            return file;
        }catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 初始化缓冲区
     * @return
     */
    public void init() {
        File file = topicFile();
        Path path = Paths.get(file.getAbsolutePath());
        try (FileChannel fileChannel = (FileChannel) Files.newByteChannel(path, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            this.mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, LEN);
            // 判断有没有数据，找到数据结尾
        }catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public int writeOffset() {
        return mappedByteBuffer.position();
    }

    private static final Integer MESSAGE_MAX_BYTES_LEN = 10;
    private static final char PAD_CHAR = '0';

    // 写操作, 返回写入的位置
    public int write(HefMessage<?> hefMessage) {
        checkState(Objects.nonNull(mappedByteBuffer), "Please init MappedByteBuffer first");
        int offset = this.mappedByteBuffer.position();
        String mess = gson.toJson(hefMessage);
        byte[] bytes = mess.getBytes(StandardCharsets.UTF_8);
        int messageLen = bytes.length;
        // 假设每条消息的长度最大长度为 10_0000_0000
        String lenPrefix = Strings.padStart(String.valueOf(messageLen), MESSAGE_MAX_BYTES_LEN, PAD_CHAR);
        String finalMessage = lenPrefix + mess;
        byte[] finalBytes = finalMessage.getBytes(StandardCharsets.UTF_8);
        int finalLen = finalBytes.length;
        // 写入内容
        Indexer.addEntry(topic, offset, finalLen);
        this.mappedByteBuffer.put(finalBytes);
        return offset;
    }

    public HefMessage<?> read(int offset) {
        checkState(Objects.nonNull(mappedByteBuffer), "Please init MappedByteBuffer first");
        ByteBuffer byteBuffer = this.mappedByteBuffer.asReadOnlyBuffer();
        byteBuffer.position(offset);
        Indexer.Entry entry = Indexer.getEntry(topic, offset);
        if (Objects.isNull(entry)) {
            return null;
        }
        byte[] bytes = new byte[entry.getLen()];
        byteBuffer.get(bytes);
        // 前十位是消息的长度
        byte[] messageBytes = Arrays.copyOfRange(bytes, MESSAGE_MAX_BYTES_LEN, bytes.length);
        String res = new String(messageBytes, StandardCharsets.UTF_8);
        TypeToken<HefMessage<?>> typeToken = new TypeToken<>(){};
        return gson.fromJson(res, typeToken.getType());
    }



    public String getTopic() {
        return topic;
    }

    public MappedByteBuffer getMappedByteBuffer() {
        return mappedByteBuffer;
    }
}
