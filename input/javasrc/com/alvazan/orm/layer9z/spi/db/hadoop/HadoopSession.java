package com.alvazan.orm.layer9z.spi.db.hadoop;

import java.util.List;
import java.util.Map;

import com.alvazan.orm.api.z8spi.BatchListener;
import com.alvazan.orm.api.z8spi.Key;
import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.MetaLookup;
import com.alvazan.orm.api.z8spi.NoSqlRawSession;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.ScanInfo;
import com.alvazan.orm.api.z8spi.action.Action;
import com.alvazan.orm.api.z8spi.action.Column;
import com.alvazan.orm.api.z8spi.action.IndexColumn;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;

public class HadoopSession implements NoSqlRawSession {

	@Override
	public void sendChanges(List<Action> actions, MetaLookup ormSession) {
	}

	@Override
	public void clearDatabase() {
		throw new UnsupportedOperationException("Not supported by actual databases.  Only can be used with in-memory db.");
	}

	@Override
	public void start(Map<String, Object> properties) {
	}

	@Override
	public void close() {
	}

	@Override
	public AbstractCursor<KeyValue<Row>> find(String colFamily,
			Iterable<byte[]> rowKeys) {
		return null;
	}

	@Override
	public AbstractCursor<Column> columnSlice(String colFamily, byte[] rowKey,
			byte[] from, byte[] to, Integer batchSize, BatchListener l) {
		return null;
	}

	@Override
	public AbstractCursor<IndexColumn> scanIndex(ScanInfo scan, Key from, Key to,
			Integer batchSize, BatchListener l) {
		return null;
	}

}
