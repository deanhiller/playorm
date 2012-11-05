package com.alvazan.play.logging;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
			List<String> keys = new ArrayList<String>();
			int initialCounter = counter;
			for(; counter < initialCounter+batchSize; counter++) {
				int postfix = counter;
				for(String name : serverNames) {
					String newName = name+postfix;
					keys.add(newName);
				}
			}
			
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
