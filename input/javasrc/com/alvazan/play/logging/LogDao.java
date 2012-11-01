package com.alvazan.play.logging;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.alvazan.orm.api.base.NoSqlEntityManager;
import com.alvazan.orm.api.base.spi.UniqueKeyGenerator;
import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.iter.Cursor;
import com.alvazan.orm.layer3.typed.IterableProxy;

public class LogDao {

	public static Iterable<LogEvent> fetchAllLogs(NoSqlEntityManager mgr, List<String> serverNames, int maxCount, int batchSize) {
		String host = UniqueKeyGenerator.getHostname();
		if(!serverNames.contains(host))
			serverNames.add(host);
		return new LogIter(mgr, serverNames, maxCount, batchSize);
	}
	
	public static Iterable<KeyValue<LogEvent>> fetchSessionLogs(NoSqlEntityManager mgr, String sessionId, int numDigits) {
		return LogEvent.findBySession(mgr, sessionId, numDigits);
	}
	
	private static class LogIter implements Iterable<LogEvent> {
		private NoSqlEntityManager mgr;
		private List<String> serverNames;
		private int maxCount;
		private int batchSize;

		public LogIter(NoSqlEntityManager mgr, List<String> serverNames,
				int maxCount, int batchSize) {
			this.mgr = mgr;
			this.serverNames = serverNames;
			this.maxCount = maxCount;
			this.batchSize = batchSize;
		}

		@Override
		public Iterator<LogEvent> iterator() {
			return new LogIterator(mgr, serverNames, maxCount, batchSize);
		}
	}
	
	private static class LogIterator implements Iterator<LogEvent> {

		private NoSqlEntityManager mgr;
		private List<String> serverNames;
		private int maxCount;
		private int batchSize;
		private Iterator<KeyValue<LogEvent>> event = null;
		private int counter = 0;
		private LogEvent nextVal;
		
		public LogIterator(NoSqlEntityManager mgr, List<String> serverNames,
				int maxCount, int batchSize) {
			this.mgr = mgr;
			this.serverNames = serverNames;
			this.maxCount = maxCount;
			this.batchSize = batchSize;
		}

		@Override
		public boolean hasNext() {
			if(counter > maxCount)
				return false;
			
			fetchBatch();
			if(!event.hasNext())
				return false;
			KeyValue<LogEvent> next = event.next();
			if(next.getValue() == null)
				return false;
			nextVal = next.getValue();
			return true;
		}

		private void fetchBatch() {
			if(event != null && event.hasNext())
				return;
			
			List<String> keys = new ArrayList<String>();
			for(String name : serverNames) {
				String newName = name+counter;
				keys.add(newName);
				counter++;
				if(counter % batchSize == 0 || counter > maxCount)
					break;
			}
			
			mgr.clear();
			
			Cursor<KeyValue<LogEvent>> cursor = mgr.findAll(LogEvent.class, keys);
			IterableProxy<KeyValue<LogEvent>> iter = new IterableProxy<KeyValue<LogEvent>>(cursor);
			event = iter.iterator();
		}

		@Override
		public LogEvent next() {
			if(!hasNext())
				throw new IllegalStateException("no more elements, check hasNext first");
			return nextVal;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("not supported");
		}
	}
}
