package com.alvazan.orm.layer9z.spi.db.hadoop;

import java.util.List;
import java.util.Map;

import com.alvazan.orm.api.spi9.db.Action;
import com.alvazan.orm.api.spi9.db.Column;
import com.alvazan.orm.api.spi9.db.Key;
import com.alvazan.orm.api.spi9.db.KeyValue;
import com.alvazan.orm.api.spi9.db.NoSqlRawSession;
import com.alvazan.orm.api.spi9.db.Row;
import com.alvazan.orm.api.spi9.db.ScanInfo;

public class HadoopSession implements NoSqlRawSession {

	@Override
	public void sendChanges(List<Action> actions, Object ormSession) {
	}

	@Override
	public void clearDatabase() {
		throw new UnsupportedOperationException("Not supported by actual databases.  Only can be used with in-memory db.");
	}

	@Override
	public void start(Map<String, Object> properties) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Iterable<Column> columnRangeScan(ScanInfo scan, Key from, Key to, int batchSize) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Iterable<KeyValue<Row>> find(String colFamily,
			Iterable<byte[]> rowKeys) {
		// TODO Auto-generated method stub
		return null;
	}

}
