package com.alvazan.orm.logging;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.inject.Named;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.z8spi.BatchListener;
import com.alvazan.orm.api.z8spi.Cache;
import com.alvazan.orm.api.z8spi.CacheThreadLocal;
import com.alvazan.orm.api.z8spi.ColumnSliceInfo;
import com.alvazan.orm.api.z8spi.Key;
import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.MetaLookup;
import com.alvazan.orm.api.z8spi.NoSqlRawSession;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.ScanInfo;
import com.alvazan.orm.api.z8spi.action.Action;
import com.alvazan.orm.api.z8spi.action.Column;
import com.alvazan.orm.api.z8spi.action.IndexColumn;
import com.alvazan.orm.api.z8spi.action.Persist;
import com.alvazan.orm.api.z8spi.action.PersistIndex;
import com.alvazan.orm.api.z8spi.action.Remove;
import com.alvazan.orm.api.z8spi.action.RemoveIndex;
import com.alvazan.orm.api.z8spi.conv.StandardConverters;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;
import com.alvazan.orm.api.z8spi.iter.DirectCursor;
import com.alvazan.orm.api.z8spi.meta.DboColumnMeta;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;

public class NoSqlRawLogger implements NoSqlRawSession {

	private static final Logger log = LoggerFactory.getLogger(NoSqlRawLogger.class);
	@Inject
	@Named("main")
	private NoSqlRawSession session;
	
	@Override
	public void sendChanges(List<Action> actions, MetaLookup ormFromAbove) {
		if(actions.size() > 10000 && log.isWarnEnabled())
			log.warn("You sure you want to send 10,000 actions in one flush....consider doing and batches and calling mgr.clear to clear out the cache every batch");
		long time = 0;
		if(log.isInfoEnabled()) {
			logInformation(actions);
			time = System.currentTimeMillis();
		}
		session.sendChanges(actions, ormFromAbove);
		if(log.isInfoEnabled()) {
			long total = System.currentTimeMillis()-time;
			log.info("[rawlogger] Sending Changes to server took(including spi plugin)="+total+" ms");
		}
	}

	private void logInformation(List<Action> actions) {
		try {
			if (log.isInfoEnabled())
				logInformationImpl(actions);
		} catch(Exception e) {
			if (log.isInfoEnabled())
				log.info("[rawlogger] (exception logging save actions, turn on trace to see)");
		}
	}
	
	private void logInformationImpl(List<Action> actions) {
		String msg = "[rawlogger] Data being flushed to database in one go=";
		for(Action act : actions) {
			DboTableMeta cf = act.getColFamily();
			if(act instanceof Persist) {
				msg += "\nCF="+cf;
				Persist p = (Persist) act;
				String key = convert(cf, p.getRowKey());
				msg += " persist rowkey="+key;
			} else if(act instanceof Remove) {
				msg += "\nCF="+cf;
				Remove r = (Remove) act;
				String key = convert(cf, r.getRowKey());
				msg += " remove  rowkey="+key;
			} else if(act instanceof PersistIndex) {
				PersistIndex p = (PersistIndex) act;
				msg += "\nCF="+p.getColFamily().getColumnFamily();
				String ind = convert(p);
				msg += " index persist("+ind;
			} else if(act instanceof RemoveIndex) {
				RemoveIndex r = (RemoveIndex) act;
				msg += "\nCF="+r.getColFamily().getColumnFamily();
				String ind = convert(r);
				msg += " index remove ("+ind;
			}
		}
		
		log.info(msg);
	}
	
	private String convert(RemoveIndex r) {
		String msg = "cf="+r.getColFamily()+")=";
		msg += "[rowkey="+StandardConverters.convertFromBytesNoExc(String.class, r.getRowKey())+"]";
		
		try {
			DboTableMeta meta = r.getColFamily();
			if(meta == null) 
				return msg+" (meta not found)";
			String colName = r.getColumn().getColumnName();
			DboColumnMeta colMeta = meta.getColumnMeta(colName);
			if(colMeta == null)
				return msg+" (table found, colmeta not found)";
		
			byte[] indexedValue = r.getColumn().getIndexedValue();
			byte[] pk = r.getColumn().getPrimaryKey();
			Object theId = meta.getIdColumnMeta().convertFromStorage2(pk);
			String idStr = meta.getIdColumnMeta().convertTypeToString(theId);
			Object valObj = colMeta.convertFromStorage2(indexedValue);
			String valStr = colMeta.convertTypeToString(valObj);
			
			return msg+"[indexval="+valStr+",to pk="+idStr+"]";
		} catch(Exception e) {
			if(log.isTraceEnabled())
				log.trace("excpetion logging", e);
			return msg + "(exception logging.  turn on trace logs to see)";
		}
	}

	private String convert(DboTableMeta meta, byte[] rowKey) {
		if(rowKey == null)
			return "null";
		
		try {
			byte[] rawKey = meta.getIdColumnMeta().unformVirtRowKey(rowKey);
			Object obj = meta.getIdColumnMeta().convertFromStorage2(rawKey);
			return meta.getIdColumnMeta().convertTypeToString(obj);
		} catch(Exception e) {
			if(log.isTraceEnabled())
				log.trace("excpetion logging", e);
			return "(exception converting, turn trace logging on to see it)";
		}
	}

	@Override
	public AbstractCursor<Column> columnSlice(ColumnSliceInfo sliceInfo, Integer batchSize, BatchListener l, MetaLookup mgr) {
		BatchListener list = l;
		if(log.isInfoEnabled()) {
			log.info("[rawlogger] CF="+sliceInfo.getColFamily()+" column slice(we have not meta info for column Slices, use scanIndex maybe?)");
			list = new LogBatchFetch("basic column slice", l, batchSize, ScanType.COLUMN_SLICE);
		}
		
		AbstractCursor<Column> ret = session.columnSlice(sliceInfo, batchSize, list, mgr);
		
		return ret;
	}
	
	@Override
	public AbstractCursor<IndexColumn> scanIndex(ScanInfo info, Key from, Key to, Integer batchSize, BatchListener l, MetaLookup mgr) {
		BatchListener list = l;
		if(log.isInfoEnabled()) {
			String cfAndIndex = logColScan(info, from, to, batchSize);
			list = new LogBatchFetch(cfAndIndex, l, batchSize, ScanType.RANGE_SLICE);
		}
		return session.scanIndex(info, from, to, batchSize, list, mgr);
	}
	
	@Override
	public AbstractCursor<IndexColumn> scanIndex(ScanInfo scanInfo, List<byte[]> values, BatchListener l, MetaLookup mgr) {
		BatchListener list = l;
		if(log.isInfoEnabled()) {
			String cfAndIndex = logColScan2(scanInfo, values);
			list = new LogBatchFetch(cfAndIndex, l, null, ScanType.NON_CONTIGUOUS);
		}

		AbstractCursor<IndexColumn> cursor = session.scanIndex(scanInfo, values, list, mgr);
		return cursor;
	}
	
	private String logColScan2(ScanInfo info, List<byte[]> values) {
		try {
			return logColScan2Impl(info, values);
		} catch(Exception e) {
			if (log.isInfoEnabled())
				log.info("[rawlogger] (Exception trying to log column scan on index cf="+info.getIndexColFamily()+" for cf="+info.getEntityColFamily());
			return info.getIndexColFamily()+" "+info.getEntityColFamily();
		}		
	}

	private String logColScan2Impl(ScanInfo info, List<byte[]> values) {
		String rowKey = StandardConverters.convertFromBytesNoExc(String.class, info.getRowKey());
		String cfAndIndex = "CF="+info.getEntityColFamily()+" index="+rowKey;
		
		String msg = cfAndIndex+"(in CF="+info.getIndexColFamily()+")";
		if(info.getEntityColFamily() == null)
			return msg + " (meta for main CF can't be looked up)";

		DboTableMeta meta = info.getEntityColFamily();
		if(meta == null)
			return msg + " (meta for main CF was not found)";
		DboColumnMeta colMeta = info.getColumnName();
		if(colMeta == null)
			return msg + " (CF meta found but columnMeta not found)";
		
		List<String> strVals = new ArrayList<String>();
		for(byte[] val : values) {
			Object fromObj = colMeta.convertFromStorage2(val);
			String str = colMeta.convertTypeToString(fromObj);
			strVals.add(str);
		}
		if (log.isInfoEnabled()) {
			msg+=" finding non-contiguous keys index rowkey="+rowKey+" for keys:"+strVals;
			log.info("[rawlogger]"+msg);
		}
		return cfAndIndex;
	}

	private String logColScan(ScanInfo info, Key from, Key to, Integer batchSize) {
		try {
			return logColScanImpl(info, from, to, batchSize);
		} catch(Exception e) {
			if (log.isInfoEnabled())
				log.info("[rawlogger] (Exception trying to log column scan on index cf="+info.getIndexColFamily()+" for cf="+info.getEntityColFamily());
			return info.getIndexColFamily()+" "+info.getEntityColFamily();
		}
	}

	private String logColScanImpl(ScanInfo info, Key from, Key to, Integer batchSize) {
		String rowKey = StandardConverters.convertFromBytesNoExc(String.class, info.getRowKey());
		String cfAndIndex = "CF="+info.getEntityColFamily()+" index="+rowKey;
		
		String msg = cfAndIndex+"(in CF="+info.getIndexColFamily()+")";
		if(info.getEntityColFamily() == null)
			return msg + " (meta for main CF can't be looked up)";

		DboTableMeta meta = info.getEntityColFamily();
		if(meta == null)
			return msg + " (meta for main CF was not found)";
		DboColumnMeta colMeta = info.getColumnName();
		if(colMeta == null)
			return msg + " (CF meta found but columnMeta not found)";
		
		String range = "";
		if(from != null) {
			Object fromObj = colMeta.convertFromStorage2(from.getKey());
			range += colMeta.convertTypeToString(fromObj);
			String firstSign = " < ";
			if(from.isInclusive())
				firstSign = " <= ";
			range += firstSign;
		}
		if(from != null || to != null) 
			range += "VALUE";
		else
			range += "ALL DATA";
		
		if(to != null) {
			String secondSign = " < ";
			if(to.isInclusive())
				secondSign = " <= ";
			range += secondSign;
			
			Object toObj = colMeta.convertFromStorage2(to.getKey());
			String toStr = colMeta.convertTypeToString(toObj);
			range += toStr;
		}
		
		if (log.isInfoEnabled()) {
			msg+=" scanning index for value in range:"+range+" with batchSize="+batchSize;
			log.info("[rawlogger]"+msg);
		}
		return cfAndIndex;
	}
	
	@Override
	public void clearDatabase() {
		long time = 0;
		if(log.isInfoEnabled()) {
			log.info("[rawlogger] CLEARING THE DATABASE!!!!(only use this method with tests or you will be screwed ;) )");
			time = System.currentTimeMillis();
		}
		session.clearDatabase();
		if(log.isInfoEnabled()) {
			long total = System.currentTimeMillis()-time;
			log.info("[rawlogger] clearDatabase took(including spi plugin)="+total+" ms");
		}
	}

	@Override
	public void start(Map<String, Object> properties) {
		if(log.isInfoEnabled()) {
			log.info("[rawlogger] starting NoSQL Service Provider and connecting");
		}
		session.start(properties);
	}

	@Override
	public void close() {
		if(log.isInfoEnabled()) {
			log.info("[rawlogger] closing NoSQL Service Provider");
		}
		session.close();
	}

	@Override
	public AbstractCursor<KeyValue<Row>> find(DboTableMeta colFamily,
			DirectCursor<byte[]> rowKeys, Cache realCache, int batchSize, BatchListener l, MetaLookup mgr) {
		BatchListener list = l;
		//NOTE: get the cache here before the below log statement which used to clear out the trhead local as it ends
		//up doing a find through the cache layer that used to clear the thread local
		Cache cache = CacheThreadLocal.getCache();
		
		if(log.isInfoEnabled()) {
			list = new LogBatchFetch("CF="+colFamily, l, batchSize, ScanType.FIND);
		}
		
		if(cache == null)
			throw new IllegalArgumentException("bug, above layer needs to call CacheThradLocal.setCache(cache) with non-null cache");
		return session.find(colFamily, rowKeys, cache, batchSize, list, mgr);
	}

	@Override
	public void readMetaAndCreateTable(MetaLookup ormSession, String colFamily) {
		session.readMetaAndCreateTable(ormSession, colFamily);
	}

	@Override
	public AbstractCursor<Row> allRows(DboTableMeta colFamily,
			MetaLookup ormSession, int batchSize) {
		return session.allRows(colFamily, ormSession, batchSize);
	}

	@Override
	public Object getDriverHelper() {
		return session.getDriverHelper();
	}
	
//	@Override
//	public AbstractCursor<KeyValue<Row>> find(String colFamily,
//			Iterable<byte[]> rKeys) {
//		//Astyanax will iterate over our iterable twice!!!! so instead we will iterate ONCE so translation
//		//only happens ONCE and then feed that to the SPI(any other spis then who iterate twice are ok as well then)
//		
//		DboTableMeta meta = null;
//		if(log.isInfoEnabled()) {
//			meta = databaseInfo.getMeta(colFamily);
//		}
//		
//		List<byte[]> allKeys = new ArrayList<byte[]>();
//		List<String> realKeys = new ArrayList<String>();
//		//This is where the cursor is read from causing queries to hit the database
//		long start = System.currentTimeMillis();
//		addToLists(rKeys, meta, allKeys, realKeys);
//		long totalTime = System.currentTimeMillis() - start;
//		if(log.isInfoEnabled())
//			log.info("[rawlogger] Reading index information took="+totalTime+" ms");
//		
//		AbstractCursor<KeyValue<Row>> ret;
//		if(log.isInfoEnabled()) {
//
//			if(allKeys.size() > 0)
//				log.info("[rawlogger] Finding keys="+realKeys);
//			long time = System.currentTimeMillis();
//			ret = session.find(colFamily, allKeys);
//			long total = System.currentTimeMillis() - time;
//			if(allKeys.size() > 0) //we really only did a find if there were actual keys passed in
//				log.info("[rawlogger] Total find keyset time(including spi plugin)="+total+" for setsize="+allKeys.size()+" keys="+realKeys+"="+total+" ms");
//			else if(log.isTraceEnabled())
//				log.trace("skipped find keyset since no keys(usually caused by cache hit)");
//		} else
//			ret = session.find(colFamily, allKeys);
//
//		//UNFORTUNATELY, astyanax's result is NOT ORDERED by the keys we provided so, we need to iterate over the whole thing here
//		//into our own List :( :( .  OTHER SPI's may not be ORDERED EITHER so we iterate here for all of them.
//		List<KeyValue<Row>> results = new ArrayList<KeyValue<Row>>();
//		Map<ByteArray, KeyValue<Row>> map = new HashMap<ByteArray, KeyValue<Row>>();
//		while(true) {
//			Holder<KeyValue<Row>> holder = ret.nextImpl();
//			if(holder == null)
//				break;
//			KeyValue<Row> kv = holder.getValue();
//			byte[] k = (byte[]) kv.getKey();
//			ByteArray b = new ByteArray(k);
//			map.put(b, kv);
//		}
//		
//		for(byte[] k : allKeys) {
//			ByteArray b = new ByteArray(k);
//			KeyValue<Row> kv = map.get(b);
//			results.add(kv);
//		}
//		
//		
//		ProxyTempCursor<KeyValue<Row>> proxy = new ProxyTempCursor<KeyValue<Row>>(results);
//		return proxy;
//	}

//	private void addToLists(Iterable<byte[]> rKeys, DboTableMeta meta,
//			List<byte[]> allKeys, List<String> realKeys) {
//		for(byte[] k : rKeys) {
//			allKeys.add(k);
//			if(log.isInfoEnabled()) {
//				try {
//					Object obj = meta.getIdColumnMeta().convertFromStorage2(k);
//					String str = meta.getIdColumnMeta().convertTypeToString(obj);
//					realKeys.add(str);
//				} catch(Exception e) {
//					log.trace("Exception occurred", e);
//					realKeys.add("[exception, turn on trace logging]");
//				}
//			}
//		}
//	}

}
