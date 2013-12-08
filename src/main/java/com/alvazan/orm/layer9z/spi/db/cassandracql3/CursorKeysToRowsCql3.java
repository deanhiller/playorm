package com.alvazan.orm.layer9z.spi.db.cassandracql3;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import javax.inject.Provider;

import com.alvazan.orm.api.z8spi.BatchListener;
import com.alvazan.orm.api.z8spi.Cache;
import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.RowHolder;
import com.alvazan.orm.api.z8spi.action.Column;
import com.alvazan.orm.api.z8spi.conv.ByteArray;
import com.alvazan.orm.api.z8spi.conv.StandardConverters;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;
import com.alvazan.orm.api.z8spi.iter.DirectCursor;
import com.alvazan.orm.api.z8spi.iter.StringLocal;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;
import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class CursorKeysToRowsCql3 extends AbstractCursor<KeyValue<Row>> {

	private DirectCursor<byte[]> rowKeys;
	private int batchSize;
	private BatchListener list;
	private Session session;
	private Info info;
	private ListIterator<KeyValue<Row>> cachedRows;
	private Provider<Row> rowProvider;
	private Cache cache;
	private DboTableMeta cf;
	private String keys;

	public CursorKeysToRowsCql3(DirectCursor<byte[]> rowKeys, int batchSize,
			BatchListener list, Provider<Row> rowProvider,
			Session session2, String keys2) {
		this.rowProvider = rowProvider;
		this.rowKeys = rowKeys;
		this.batchSize = batchSize;
		this.list = list;
		this.session = session2;
		this.keys = keys2;
	}

	@Override
	public String toString() {
		String tabs = StringLocal.getAndAdd();
		String keys = "" + rowKeys;
		if (rowKeys instanceof List)
			keys = "List" + keys;
		String retVal = "CursorKeysToRowsMDB[" + tabs + keys
				+ tabs + "]";
		StringLocal.set(tabs.length());
		return retVal;
	}

	public void setupMore(DboTableMeta cf2, Cache cache2) {
		if (cache2 == null || cf2 == null)
			throw new IllegalArgumentException(
					"no params can be null but one was null");
		this.cf = cf2;
		this.cache = cache2;
		beforeFirst();
	}

	@Override
	public void beforeFirst() {
		rowKeys.beforeFirst();
		cachedRows = null;
	}

	@Override
	public void afterLast() {
		rowKeys.afterLast();
		cachedRows = null;
	}

	@Override
	public com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder<KeyValue<Row>> nextImpl() {
		loadCache();
		if (cachedRows == null || !cachedRows.hasNext())
			return null;

		return new Holder<KeyValue<Row>>(cachedRows.next());
	}

	@Override
	public com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder<KeyValue<Row>> previousImpl() {
		loadCacheBackward();
		if (cachedRows == null || !cachedRows.hasPrevious())
			return null;

		return new Holder<KeyValue<Row>>(cachedRows.previous());
	}

	private void loadCache() {
		if (cachedRows != null && cachedRows.hasNext())
			return; // There are more rows so return and the code will return
					// the next result from cache

		List<RowHolder<Row>> results = new ArrayList<RowHolder<Row>>();
		List<byte[]> keysToLookup = new ArrayList<byte[]>();
		while (results.size() < batchSize) {
			Holder<byte[]> keyHolder = rowKeys.nextImpl();
			if (keyHolder == null)
				break; // we are officially exhausted
			byte[] nextKey = keyHolder.getValue();
			if (cache != null) {
				RowHolder<Row> result = cache.fromCache(cf, nextKey);
				if (result == null)
				    keysToLookup.add(nextKey);
				results.add(result);
			}

		}
		
		ResultSet resultSet = null;

        if (keysToLookup.size() > 0) {
            //String[] keyStrings = new String[keysToLookup.size()];
            ByteBuffer[] keyStrings = new ByteBuffer[keysToLookup.size()];
            int count = 0;
            for (byte[] rowKey : keysToLookup) {
                //keyStrings[count] = StandardConverters.convertFromBytes(String.class, rowKey);
                keyStrings[count] = ByteBuffer.wrap(rowKey);
                count++;
            }

            if (list != null)
                list.beforeFetchingNextBatch();
            try {
                Clause inClause = QueryBuilder.in("id", keyStrings);
                //change the batchsize
                Query query = QueryBuilder.select().all().from(keys, cf.getColumnFamily()).where(inClause).limit(100);
                resultSet = session.execute(query);
            } catch (Exception e) {
                System.out.println(" Exception:" + e.getMessage());
            }
            if (list != null) list.afterFetchingNextBatch(batchSize);
        }

		Map<ByteArray, KeyValue<Row>> map = new HashMap<ByteArray, KeyValue<Row>>();

		fillCache(map, resultSet, keysToLookup);

		// This is copied from Cassandra. Need to check how can we get results in an order.

		List<KeyValue<Row>> finalRes = new ArrayList<KeyValue<Row>>();
		Iterator<byte[]> keyIter = keysToLookup.iterator();
		for (RowHolder<Row> r : results) {
			if (r == null) {
				byte[] key = keyIter.next();
				ByteArray b = new ByteArray(key);
				KeyValue<Row> kv = map.get(b);
				if (kv!=null)
					finalRes.add(kv);
			} else {
				Row row = r.getValue();
				KeyValue<Row> kv = new KeyValue<Row>();
				kv.setKey(r.getKey());
				kv.setValue(row);
				finalRes.add(kv);
			}
		}

		cachedRows = finalRes.listIterator();
	}

	private void loadCacheBackward() {
		if (cachedRows != null && cachedRows.hasPrevious())
			return; // There are more rows so return and the code will return
					// the next result from cache

		List<RowHolder<Row>> results = new ArrayList<RowHolder<Row>>();
		List<byte[]> keysToLookup = new ArrayList<byte[]>();
		while (results.size() < batchSize) {
			Holder<byte[]> keyHolder = rowKeys.previousImpl();
			if (keyHolder == null)
				break; // we are officially exhausted

			byte[] previousKey = keyHolder.getValue();
			RowHolder<Row> result = cache.fromCache(cf, previousKey);
			if (result == null)
				keysToLookup.add(0, previousKey);

			results.add(result);
		}
        ResultSet resultSet = null;

        if (keysToLookup.size() > 0) {
            //String[] keyStrings = new String[keysToLookup.size()];
            ByteBuffer[] keyStrings = new ByteBuffer[keysToLookup.size()];
            int count = 0;
            for (byte[] rowKey : keysToLookup) {
                //keyStrings[count] = StandardConverters.convertFromBytes(String.class, rowKey);
                keyStrings[count] = ByteBuffer.wrap(rowKey);
                count++;
            }

            if (list != null)
                list.beforeFetchingNextBatch();
            try {
                // CHANGE THE BATCHSIZE
                Clause inClause = QueryBuilder.in("id", keyStrings);
                Query query = QueryBuilder.select().all().from(keys, cf.getColumnFamily()).where(inClause).limit(100);
                resultSet = session.execute(query);
            } catch (Exception e) {
                System.out.println(" Exception:" + e.getMessage());
            }
            if (list != null) list.afterFetchingNextBatch(batchSize);
        }
		Map<ByteArray, KeyValue<Row>> map = new HashMap<ByteArray, KeyValue<Row>>();

        fillCache(map, resultSet, keysToLookup);


		List<KeyValue<Row>> finalRes = new ArrayList<KeyValue<Row>>();
		Iterator<byte[]> keyIter = keysToLookup.iterator();
		for (RowHolder<Row> r : results) {
			if (r == null) {
				byte[] key = keyIter.next();
				ByteArray b = new ByteArray(key);
				KeyValue<Row> kv = map.get(b);
				if (kv != null)
					finalRes.add(kv);
			} else {
				Row row = r.getValue();
				KeyValue<Row> kv = new KeyValue<Row>();
				kv.setKey(r.getKey());
				kv.setValue(row);
				finalRes.add(kv);
			}
		}

		cachedRows = finalRes.listIterator();
		while (cachedRows.hasNext())
			cachedRows.next();
	}

    private void fillCache(Map<ByteArray, KeyValue<Row>> map, ResultSet cursor, List<byte[]> keysToLookup) {
        byte[] rowKey = null;
        List<List<com.datastax.driver.core.Row>> cqlRows = new ArrayList<List<com.datastax.driver.core.Row>>();
        List<com.datastax.driver.core.Row> actualRowList = new ArrayList<com.datastax.driver.core.Row>();
        if (cursor == null) {
            for (byte[] key : keysToLookup) {
                KeyValue<Row> kv = new KeyValue<Row>();
                kv.setKey(key);
                kv.setValue(null);
                ByteArray b = new ByteArray(key);
                map.put(b, kv);
                cache.cacheRow(cf, key, kv.getValue());
            }
        } else {
            for (com.datastax.driver.core.Row cqlRow : cursor) {
                ByteBuffer data = cqlRow.getBytes("id");
                byte[] val = new byte[data.remaining()];
                data.get(val);

                if (Arrays.equals(val, rowKey)) {
                    actualRowList.add(cqlRow);
                } else {
                    if (rowKey != null)
                        cqlRows.add(actualRowList);
                    rowKey = val;
                    actualRowList = new ArrayList<com.datastax.driver.core.Row>();
                    actualRowList.add(cqlRow);
                }
            }
            cqlRows.add(actualRowList);

            for (List<com.datastax.driver.core.Row> actualRow : cqlRows) {
                KeyValue<Row> kv = new KeyValue<Row>();
                Row r = rowProvider.get();
                byte[] cqlRowKey = null;
                for (com.datastax.driver.core.Row cqlRow : actualRow) {
                    ByteBuffer cqlRowKeyData = cqlRow.getBytes("id");
                    cqlRowKey = new byte[cqlRowKeyData.remaining()];
                    cqlRowKeyData.get(cqlRowKey);

                    kv.setKey(cqlRowKey);
                    r.setKey(cqlRowKey);
                    byte[] name = StandardConverters.convertToBytes(cqlRow.getString("colname"));
                    ByteBuffer data = cqlRow.getBytes("colvalue");
                    byte[] val = new byte[data.remaining()];
                    data.get(val);
                    Column c = new Column();
                    c.setName(name);
                    if (val.length != 0)
                        c.setValue(val);
                    r.put(c);

                    kv.setValue(r);
                    ByteArray b = new ByteArray(cqlRowKey);
                    map.put(b, kv);
                    cache.cacheRow(cf, cqlRowKey, kv.getValue());
                }
            }

            // Now put the remaining keys which are not in CQL3's cursor.
            // This is because Cassandra returns all the rows with rowkeys while CQL# doesn't
            for (byte[] key : keysToLookup) {
                ByteArray baKey = new ByteArray(key);
                if (!map.containsKey(baKey)) {
                    KeyValue<Row> kv = new KeyValue<Row>();
                    kv.setKey(key);
                    kv.setValue(null);
                    // ByteArray b = new ByteArray(key);
                    map.put(baKey, kv);
                    cache.cacheRow(cf, key, kv.getValue());
                }
            }

        }
    }

}
