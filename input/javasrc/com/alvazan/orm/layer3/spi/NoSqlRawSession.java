package com.alvazan.orm.layer3.spi;

import java.util.List;

import com.alvazan.orm.layer2.nosql.Row;

public interface NoSqlRawSession {
	
	public List<Row> find(String colFamily, List<byte[]> key);
	
	/**
	 * Action is subclassed by Remove and Persist and will be executed
	 * in the order we are given here
	 * @param actions
	 */
	public void sendChanges(List<Action> actions);
}
