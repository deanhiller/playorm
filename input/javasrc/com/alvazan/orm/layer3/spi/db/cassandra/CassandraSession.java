package com.alvazan.orm.layer3.spi.db.cassandra;

import java.util.List;

import com.alvazan.orm.layer2.nosql.Row;
import com.alvazan.orm.layer3.spi.db.Action;
import com.alvazan.orm.layer3.spi.db.NoSqlRawSession;

public class CassandraSession implements NoSqlRawSession {

	@Override
	public List<Row> find(String colFamily, List<byte[]> key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void sendChanges(List<Action> actions) {
		// TODO Auto-generated method stub
		
	}


}
