package com.alvazan.orm.logging;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.inject.Named;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.spi3.meta.DboColumnMeta;
import com.alvazan.orm.api.spi3.meta.DboDatabaseMeta;
import com.alvazan.orm.api.spi3.meta.DboTableMeta;
import com.alvazan.orm.api.spi3.meta.conv.StandardConverters;
import com.alvazan.orm.api.spi9.db.Action;
import com.alvazan.orm.api.spi9.db.Column;
import com.alvazan.orm.api.spi9.db.Key;
import com.alvazan.orm.api.spi9.db.KeyValue;
import com.alvazan.orm.api.spi9.db.NoSqlRawSession;
import com.alvazan.orm.api.spi9.db.Persist;
import com.alvazan.orm.api.spi9.db.PersistIndex;
import com.alvazan.orm.api.spi9.db.Remove;
import com.alvazan.orm.api.spi9.db.RemoveIndex;
import com.alvazan.orm.api.spi9.db.Row;
import com.alvazan.orm.api.spi9.db.ScanInfo;

public class NoSqlRawLogger implements NoSqlRawSession {

	private static final Logger log = LoggerFactory.getLogger(NoSqlRawLogger.class);
	@Inject
	@Named("main")
	private NoSqlRawSession session;
	@Inject
	private DboDatabaseMeta databaseInfo;
	
	@Override
	public void sendChanges(List<Action> actions, Object ormFromAbove) {
		if(log.isInfoEnabled()) {
			logInformation(actions);
		}
		session.sendChanges(actions, ormFromAbove);
	}

	private void logInformation(List<Action> actions) {
		try {
			logInformationImpl(actions);
		} catch(Exception e) {
			log.info("[rawlogger] (exception logging save actions, turn on trace to see)");
		}
	}
	private void logInformationImpl(List<Action> actions) {
		String msg = "[rawlogger] Data being flushed to database in one go=";
		for(Action act : actions) {
			String cf = act.getColFamily();
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
				msg += "\nCF="+p.getRealColFamily();
				String ind = convert(p);
				msg += " index persist("+ind;
			} else if(act instanceof RemoveIndex) {
				RemoveIndex r = (RemoveIndex) act;
				msg += "\nCF="+r.getRealColFamily();
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
			DboTableMeta meta = databaseInfo.getMeta(r.getRealColFamily());
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

	private String convert(String cf, byte[] rowKey) {
		if(rowKey == null)
			return "null";
		
		DboTableMeta meta = databaseInfo.getMeta(cf);
		if(meta == null)
			return "(meta not found)";

		try {
			Object obj = meta.getIdColumnMeta().convertFromStorage2(rowKey);
			return meta.getIdColumnMeta().convertTypeToString(obj);
		} catch(Exception e) {
			if(log.isTraceEnabled())
				log.trace("excpetion logging", e);
			return "(exception converting, turn trace logging on to see it)";
		}
	}

	@Override
	public Iterable<Column> columnRangeScan(ScanInfo info, Key from, Key to, int batchSize) {
		if(log.isInfoEnabled()) {
			logColScan(info, from, to, batchSize);
		}
		return session.columnRangeScan(info, from, to, batchSize);
	}
	
	private void logColScan(ScanInfo info, Key from, Key to, int batchSize) {
		try {
			String msg = logColScanImpl(info, from, to, batchSize);
			log.info("[rawlogger]"+msg);
		} catch(Exception e) {
			log.info("[rawlogger] (Exception trying to log column scan on index cf="+info.getIndexColFamily()+" for cf="+info.getEntityColFamily());
		}
	}

	private String logColScanImpl(ScanInfo info, Key from, Key to, int batchSize) {
		String msg = "main CF="+info.getEntityColFamily()+" index CF="+info.getIndexColFamily();
		if(info.getEntityColFamily() == null)
			return msg + " (meta for main CF can't be looked up)";

		DboTableMeta meta = databaseInfo.getMeta(info.getEntityColFamily());
		if(meta == null)
			return msg + " (meta for main CF was not found)";
		DboColumnMeta colMeta = meta.getColumnMeta(info.getColumnName());
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
		
		String rowKey = StandardConverters.convertFromBytesNoExc(String.class, info.getRowKey());
		
		return msg+" scanning index rowkey="+rowKey+" for value in range:"+range+" with batchSize="+batchSize;
	}
	
	@Override
	public void clearDatabase() {
		if(log.isInfoEnabled()) {
			log.info("[rawlogger] clearing database");
		}
		session.clearDatabase();
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
	public Iterable<KeyValue<Row>> find(String colFamily,
			Iterable<byte[]> rowKeys) {
		//This iterable allows us to log inline so we don't for loop until the bottom with everyone
		//else...We do ONE LOOP at the bottom on all iterators that were proxied up.
		Iterable<byte[]> iterProxy = new IterLogProxy("[rawlogger]", databaseInfo, colFamily, rowKeys);
		if(log.isInfoEnabled())
			return session.find(colFamily, iterProxy);
		else
			return session.find(colFamily, rowKeys);
	}

}
