package com.alvazan.orm.layer3.spi.hadoop;

import java.util.List;

import com.alvazan.orm.layer2.nosql.Row;
import com.alvazan.orm.layer3.spi.Action;
import com.alvazan.orm.layer3.spi.NoSqlRawSession;

public class HadoopSession implements NoSqlRawSession {

	@Override
	public List<Row> find(String colFamily, List<byte[]> key) {
		return null;
	}

	@Override
	public void sendChanges(List<Action> actions) {
	}
}
