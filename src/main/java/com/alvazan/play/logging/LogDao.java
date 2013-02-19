package com.alvazan.play.logging;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.joda.time.LocalDateTime;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.spi.UniqueKeyGenerator;
import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;
import com.alvazan.orm.api.z8spi.iter.Cursor;
import com.alvazan.play.NoSql;

public class LogDao {

	public static Cursor<LogEvent> fetchAllLogs(NoSqlEntityManager mgr, int batchSize) {
		ServersThatLog servers = NoSql.em().find(ServersThatLog.class, ServersThatLog.THE_ONE_KEY);
		
		List<String> serverNames = new ArrayList<String>();
		if(servers != null)
			serverNames = servers.getServers();
		
		String host = UniqueKeyGenerator.getHostname();
		if(!serverNames.contains(host))
			serverNames.add(host);
		return new LogIterator(mgr, serverNames, batchSize);
	}
	
	public static Cursor<KeyValue<LogEvent>> fetchLatestLogs(NoSqlEntityManager mgr, int count) {
		ServersThatLog servers = NoSql.em().find(ServersThatLog.class, ServersThatLog.THE_ONE_KEY);
		
		List<String> serverNames = new ArrayList<String>();
		String serverName = null;
		if(servers != null) {
			serverNames = servers.getServers();
			serverName = servers.getServers().get(0);
		}
		
		Integer index = null;
		int row = 0;
		Answer t = new Answer(null, row);
		do {
			findLatestRow(mgr, serverName, t);
			index = t.getAnswer();
			row = t.getRow();
		} while(index == null);
		
		int min = Math.max(0, index-count);
		int max = index;
		List<String> keys = createKeys(serverNames, min, max);
		return mgr.findAll(LogEvent.class, keys);
	}

	private static List<String> createKeys(List<String> serverNames, int min, int max) {
		List<String> keys = new ArrayList<String>();
		for(int i = min; i < max; i++) {
			for(String name : serverNames) {
				String newName = name+i;
				keys.add(newName);
			}
		}
		return keys;
	}

	private static void findLatestRow(NoSqlEntityManager mgr, String serverName, Answer ans) {
		int row = ans.getRow();
		List<String> keys = new ArrayList<String>();
		for(int i = row; i < 200+row; i++) {
			String name = serverName + i;
			keys.add(name);
		}
		mgr.clear();

		Cursor<KeyValue<LogEvent>> cursor = mgr.findAll(LogEvent.class, keys);
		
		long time = 0;
		LocalDateTime current = new LocalDateTime(time);
		
		while(cursor.next()) {
			KeyValue<LogEvent> kv = cursor.getCurrent();
			LogEvent value = kv.getValue();
			if(value == null) {
				ans.setAnswer(row);
				return;
			}
			LocalDateTime t1 = value.getTime();
			if(current.isAfter(t1)) {
				ans.setAnswer(row);
				return;
			}
			current = t1;
			row++;
		}
		
		ans.setRow(row);
	}
	
	private static Iterable<KeyValue<LogEvent>> fetchSessionLogs(NoSqlEntityManager mgr, String sessionId, int numDigits) {
		return LogEvent.findBySession(mgr, sessionId, numDigits);
	}

	public static List<LogEvent> fetchOrderedSessionLogs(NoSqlEntityManager mgr, String sessionId, int numDigits) {
		Iterable<KeyValue<LogEvent>> iter = fetchSessionLogs(mgr, sessionId, numDigits);
		
		List<LogEvent> events = new ArrayList<LogEvent>();
		for(KeyValue<LogEvent> evts : iter) {
			if(evts.getValue() != null) {
				events.add(evts.getValue());
			}
		}
		
		Collections.sort(events, new ByDate());
		
		return events;
	}
	
	private static class LogIterator extends AbstractCursor<LogEvent> {

		private NoSqlEntityManager mgr;
		private List<String> serverNames;
		private int batchSize;
		private AbstractCursor<KeyValue<LogEvent>> event = null;
		private int counter = 0;
		
		public LogIterator(NoSqlEntityManager mgr, List<String> serverNames, int batchSize) {
			this.mgr = mgr;
			this.serverNames = serverNames;
			this.batchSize = batchSize;
		}

		@Override
		public void beforeFirst() {
			counter = 0;
			fetchNewBatch();
		}
		
		//TODO:JSC  none of this works if I can't figure out what the last index/size/number of results is...  
		@Override
		public void afterLast() {
			counter = 0;  //<--  JSC here
			fetchNewBatchFromEnd();
		}

		@Override
		public com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder<LogEvent> nextImpl() {
			if(event == null)
				fetchNewBatch();
			
			Holder<KeyValue<LogEvent>> nextImpl = event.nextImpl();
			if(nextImpl == null) {
				fetchNewBatch();
				nextImpl = event.nextImpl();
			}
			
			if(nextImpl == null)
				return null;
			KeyValue<LogEvent> kv = nextImpl.getValue();
			if(kv.getValue() == null)
				return null;
			
			return new Holder<LogEvent>(kv.getValue());
		}
		
		@Override
		public com.alvazan.orm.api.z8spi.iter.AbstractCursor.Holder<LogEvent> previousImpl() {
			if(event == null)
				fetchNewBatchFromEnd();
			
			Holder<KeyValue<LogEvent>> prevImpl = event.previousImpl();
			if(prevImpl == null) {
				fetchNewBatchFromEnd();
				prevImpl = event.previousImpl();
			}
			
			if(prevImpl == null)
				return null;
			KeyValue<LogEvent> kv = prevImpl.getValue();
			if(kv.getValue() == null)
				return null;
			
			return new Holder<LogEvent>(kv.getValue());
		}
		
//		@Override
//		public boolean hasNext() {
//			log.info("has next being called");
//			if(nextVal != null)
//				return true;
//			else if(counter > maxCount)
//				return false;
//			
//			log.info("fetch batch maybe");
//			fetchBatch();
//			if(!event.hasNext())
//				return false;
//			KeyValue<LogEvent> next = event.next();
//			if(next.getValue() == null)
//				return false;
//			nextVal = next.getValue();
//			return true;
//		}

		private void fetchNewBatch() {
			List<String> keys = createKeys(serverNames, counter, counter+batchSize);
			counter = counter+batchSize;
			mgr.clear();
			
			event = (AbstractCursor<KeyValue<LogEvent>>) mgr.findAll(LogEvent.class, keys);
		}

		private void fetchNewBatchFromEnd() {
			List<String> keys = createKeys(serverNames, counter-batchSize, counter);
			counter = counter-batchSize;
			mgr.clear();
			
			event = (AbstractCursor<KeyValue<LogEvent>>) mgr.findAll(LogEvent.class, keys);
		}
//		@Override
//		public LogEvent next() {
//			if(!hasNext())
//				throw new IllegalStateException("no more elements, check hasNext first");
//			LogEvent temp = nextVal;
//			nextVal = null;
//			return temp;
//		}
//
//		@Override
//		public void remove() {
//			throw new UnsupportedOperationException("not supported");
//		}
	}
}
