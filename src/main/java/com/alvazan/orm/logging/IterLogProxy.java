package com.alvazan.orm.logging;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.z8spi.meta.DboDatabaseMeta;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;

public class IterLogProxy implements Iterable<byte[]> {

	private static final Logger log = LoggerFactory.getLogger(IterLogProxy.class);
	private String prefix;
	private DboDatabaseMeta databaseInfo;
	private String colFamily;
	private Iterable<byte[]> rowKeys;

	public IterLogProxy(String prefix, DboDatabaseMeta databaseInfo,
			String colFamily, Iterable<byte[]> rowKeys) {
		this.prefix = prefix;
		this.databaseInfo = databaseInfo;
		this.colFamily = colFamily;
		this.rowKeys = rowKeys;
	}

	@Override
	public Iterator<byte[]> iterator() {
		return new IteratorLogProxy(prefix, databaseInfo, colFamily, rowKeys.iterator());
	}

	private static class IteratorLogProxy implements Iterator<byte[]> {
		
		private String prefix;
		private String colFamily;
		private Iterator<byte[]> rowKeys;
		private List<String> realKeys = new ArrayList<String>();
		private DboTableMeta meta;
		private boolean alreadyLogged = false;
		public IteratorLogProxy(String prefix, DboDatabaseMeta databaseInfo,
				String colFamily, Iterator<byte[]> rowKeys) {
			this.prefix = prefix;
			this.colFamily = colFamily;
			this.rowKeys = rowKeys;
			meta = databaseInfo.getMeta(colFamily);
		}

		@Override
		public boolean hasNext() {
			boolean hasNext = rowKeys.hasNext();
			if(log.isInfoEnabled() && !hasNext && !alreadyLogged && realKeys.size() > 0) {
				//we are finished and can now log the keys about to be looked up
				log.info(prefix+" CF="+colFamily+" finding keys="+realKeys);
				alreadyLogged = true;
			}
			return hasNext;
		}

		@Override
		public byte[] next() {
			byte[] k = rowKeys.next();
			if(meta != null && log.isInfoEnabled()) {
				try {
					Object obj = meta.getIdColumnMeta().convertFromStorage2(k);
					String str = meta.getIdColumnMeta().convertTypeToString(obj);
					realKeys.add(str);
				} catch(Exception e) {
					if(log.isTraceEnabled())
						log.trace("Exception occurred", e);
					realKeys.add("[exception, turn on trace logging]");
				}
			}
			
			return k;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("not supported and probably never will be");
		}
	}
}
