package com.alvazan.test.orm.layer3.spi.index.inmemory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.alvazan.orm.api.spi.index.IndexAdd;
import com.alvazan.orm.api.spi.index.IndexReaderWriter;
import com.alvazan.orm.layer3.spi.index.inmemory.MemoryIndexWriter;

/**
 * 
 * @author Brian
 * 
 */
public class MemoryIndexWriterTest {
    @Test
    public void test() {
        MemoryIndexWriter writer = new MemoryIndexWriter();
        Map<String, List<IndexAdd>> addToIndex = new HashMap<String, List<IndexAdd>>();
        IndexAdd indexAdd = new IndexAdd();
        Map<String, String> map = new HashMap<String, String>();
        map.put(IndexReaderWriter.IDKEY, "1");
        indexAdd.setItem(map);
        addToIndex.put("key", Arrays.asList(indexAdd));
        writer.sendAdds(addToIndex);
    }
}
