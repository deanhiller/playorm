package com.alvazan.orm.layer5.nosql.cache;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.z5api.NoSqlSession;
import com.alvazan.orm.api.z8spi.Cache;
import com.alvazan.orm.api.z8spi.CacheThreadLocal;
import com.alvazan.orm.api.z8spi.Key;
import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.MetaLookup;
import com.alvazan.orm.api.z8spi.NoSqlRawSession;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.RowHolder;
import com.alvazan.orm.api.z8spi.ScanInfo;
import com.alvazan.orm.api.z8spi.action.Column;
import com.alvazan.orm.api.z8spi.action.IndexColumn;
import com.alvazan.orm.api.z8spi.conv.ByteArray;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;
import com.alvazan.orm.api.z8spi.iter.DirectCursor;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;

public class NoSqlReadCacheImpl implements NoSqlSession, Cache {

	private static final Logger log = LoggerFactory.getLogger(NoSqlReadCacheImpl.class);
	
	@Inject @Named("writecachelayer")
	private NoSqlSession session;
	private Map<TheKey, RowHolder<Row>> cache = new HashMap<TheKey, RowHolder<Row>>();
	@Inject
	private Provider<Row> rowProvider;
	
	@Override
	public void put(DboTableMeta colFamily, byte[] rowKey, List<Column> columns) {
		session.put(colFamily, rowKey, columns);
		RowHolder<Row> currentRow = fromCache(colFamily, rowKey);
		if(currentRow == null) {
			currentRow = new RowHolder<Row>(rowKey);
		}

		Row value = currentRow.getValue();
		if(value == null)
			value = rowProvider.get();
		
		value.setKey(rowKey);
		value.addColumns(columns);
		cacheRow(colFamily, rowKey, value);
	}

	@Override
	public void persistIndex(DboTableMeta colFamily, String indexColFamily, byte[] rowKey, IndexColumn columns) {
		if(indexColFamily == null)
			throw new IllegalArgumentException("indexcolFamily cannot be null");
		else if(rowKey == null)
			throw new IllegalArgumentException("rowKey cannot be null");
		else if(columns == null)
			throw new IllegalArgumentException("column cannot be null");
		session.persistIndex(colFamily, indexColFamily, rowKey, columns);
	}
	
	@Override
	public void remove(DboTableMeta colFamily, byte[] rowKey) {
		session.remove(colFamily, rowKey);
		cacheRow(colFamily, rowKey, null);
	}

	@Override
	public void remove(DboTableMeta colFamily, byte[] rowKey, Collection<byte[]> columnNames) {
		session.remove(colFamily, rowKey, columnNames);
		RowHolder<Row> currentRow = fromCache(colFamily, rowKey);
		if(currentRow == null) {
			return;
		}
		Row value = currentRow.getValue();
		if(value == null) {
			return;
		}
		
		value.removeColumns(columnNames);
	}

	@Override
	public Row find(DboTableMeta colFamily, byte[] rowKey) {
		RowHolder<Row> result = fromCache(colFamily, rowKey);
		if(result != null)
			return result.getValue(); //This may return the cached null value!!
		
		Row row = session.find(colFamily, rowKey);
		cacheRow(colFamily, rowKey, row);
		return row;
	}
	
	@Override
	public AbstractCursor<KeyValue<Row>> find(DboTableMeta colFamily,
			DirectCursor<byte[]> rowKeys, boolean skipCache, boolean cacheResults, Integer batchSize) {
		Cache c = new EmptyCache(this, skipCache, cacheResults);
		
		//NOTE: I would put a finally to clear out the threadlocal normally BUT sometimes log statements may
		//cause further finds to be called which come in here as well and on their way BACK up the stack, they set
		//the cache to NULL before the find actually happens(ie. very bad).
		CacheThreadLocal.setCache(c);

		//A layer below will read the thread local and pass it to lowest layer to use
		AbstractCursor<KeyValue<Row>> rowsFromDb = session.find(colFamily, rowKeys, skipCache, cacheResults, batchSize);
		return rowsFromDb;
	}
	
	public RowHolder<Row> fromCache(DboTableMeta colFamily, byte[] key) {
		if(key == null)
			throw new IllegalArgumentException("CF="+colFamily+" key is null and shouldn't be....(this should be trapped in higher level exception telling us which index is corrupt");
		TheKey k = new TheKey(colFamily.getColumnFamily(), key);
		RowHolder<Row> holder = cache.get(k);
		if(holder != null && log.isInfoEnabled())
			log.info("cache hit(need to optimize this even further)");
		return holder;
	}

	public void cacheRow(DboTableMeta colFamily, byte[] key, Row r) {
		//NOTE: We cache null rows as on a user.getYYYEntites(), the loaded entities may be null though the user
		//get a List<YYYEntity> and all are there but he can check if they are really there with
		//mgr.checkRowExists(entity) and that will just hit the cache
		TheKey k = new TheKey(colFamily.getColumnFamily(), key);
		RowHolder<Row> holder = new RowHolder<Row>(key, r); //r may be null so we are caching null here
		cache.put(k, holder);
	}

	static final class TheKey {
		private final String colFamily;
		private final ByteArray key;
        private final int hash;
		
		TheKey(String cf, byte[] key) {
			colFamily = cf;
			this.key = new ByteArray(key);
            this.hash = calcHash();
		}

        private int calcHash() {
            final int prime = 31;
            int result = 1;
            result = prime * result
                     + ((colFamily == null) ? 0 : colFamily.hashCode());
            result = prime * result + ((key == null) ? 0 : key.hashCode());
            return result;
        }

        @Override
        public int hashCode() {
            return hash;
        }

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			TheKey other = (TheKey) obj;
			if (colFamily == null) {
				if (other.colFamily != null)
					return false;
			} else if (!colFamily.equals(other.colFamily))
				return false;
			if (key == null) {
				if (other.key != null)
					return false;
			} else if (!key.equals(other.key))
				return false;
			return true;
		}
		
	}
	
	@Override
	public void flush() {
		session.flush();
	}

	@Override
	public NoSqlRawSession getRawSession() {
		return session.getRawSession();
	}

	@Override
	public void removeFromIndex(DboTableMeta cf, String indexColFamily, byte[] rowKeyBytes,
			IndexColumn c) {
		session.removeFromIndex(cf, indexColFamily, rowKeyBytes, c);
	}
	
	@Override
	public void clearDb() {
		session.clearDb();
	}

	@Override
	public AbstractCursor<Column> columnSlice(DboTableMeta colFamily, byte[] rowKey, byte[] from, byte[] to, Integer batchSize) {
		return session.columnSlice(colFamily, rowKey, from, to, batchSize);
	}
	
	@Override
	public AbstractCursor<IndexColumn> scanIndex(ScanInfo info, Key from, Key to, Integer batchSize) {
		return session.scanIndex(info, from, to, batchSize);
	}

	@Override
	public void setOrmSessionForMeta(MetaLookup ormSession) {
		session.setOrmSessionForMeta(ormSession);
	}

	@Override
	public AbstractCursor<IndexColumn> scanIndex(ScanInfo scanInfo, List<byte[]> values) {
		return session.scanIndex(scanInfo, values);
	}

	@Override
	public void clear() {
		cache.clear();
	}

	@Override
	public void removeColumn(DboTableMeta colFamily, byte[] rowKey,
			byte[] columnName) {
		session.removeColumn(colFamily, rowKey, columnName);
		cacheRow(colFamily, rowKey, null);
	}

	@Override
	public AbstractCursor<Row> allRows(DboTableMeta colFamily, int batchSize) {
		return session.allRows(colFamily, batchSize);
	}

}
