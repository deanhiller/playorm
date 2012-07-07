package com.alvazan.orm.layer3.spi.db.hadoop;

import java.util.List;

import com.alvazan.orm.api.spi.db.Action;
import com.alvazan.orm.api.spi.db.NoSqlRawSession;
import com.alvazan.orm.api.spi.db.Row;

public class HadoopSession implements NoSqlRawSession {

	@Override
	public List<Row> find(String colFamily, List<byte[]> key) {
		return null;
	}

	@Override
	public void sendChanges(List<Action> actions) {
	}
}
