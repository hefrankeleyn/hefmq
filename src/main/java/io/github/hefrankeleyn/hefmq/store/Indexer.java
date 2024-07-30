package io.github.hefrankeleyn.hefmq.store;

import com.google.common.collect.Maps;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.util.List;
import java.util.Map;

/**
 * @Date 2024/7/30
 * @Author lifei
 */
public class Indexer {

    private static final MultiValueMap<String, Entry> topicEntryMap = new LinkedMultiValueMap<>();
    private static final Map<Integer, Entry> offsetEntryMap = Maps.newHashMap();

    public static class Entry {
        private Integer offset;
        private Integer len;

        public Entry() {
        }

        public Entry(Integer offset, Integer len) {
            this.offset = offset;
            this.len = len;
        }

        public Integer getOffset() {
            return offset;
        }

        public void setOffset(Integer offset) {
            this.offset = offset;
        }

        public Integer getLen() {
            return len;
        }

        public void setLen(Integer len) {
            this.len = len;
        }
    }


    public static void addEntry(String topic, Integer offset, Integer len) {
        Entry entry = new Entry(offset, len);
        topicEntryMap.add(topic, entry);
        offsetEntryMap.put(entry.getOffset(), entry);
    }

    public static List<Entry> getEntries(String topic) {
        return topicEntryMap.get(topic);
    }

    public static Entry getEntry(Integer offset) {
        return offsetEntryMap.get(offset);
    }
}
