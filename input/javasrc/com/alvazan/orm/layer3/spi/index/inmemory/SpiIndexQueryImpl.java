package com.alvazan.orm.layer3.spi.index.inmemory;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.spi2.NoSqlSession;
import com.alvazan.orm.api.spi3.db.Column;
import com.alvazan.orm.api.spi3.index.ExpressionNode;
import com.alvazan.orm.api.spi3.index.SpiQueryAdapter;
import com.alvazan.orm.api.spi3.index.StateAttribute;
import com.alvazan.orm.api.spi3.index.ValAndType;
import com.alvazan.orm.parser.antlr.NoSqlLexer;

public class SpiIndexQueryImpl implements SpiQueryAdapter {

	private static final Logger log = LoggerFactory.getLogger(SpiIndexQueryImpl.class);

	private static final int BATCH_SIZE = 2000;
	
	private String indexName;
	private SpiMetaQueryImpl spiMeta;
	private NoSqlSession session;
	private Map<String, ValAndType> parameters = new HashMap<String, ValAndType>();
	
	public void setup(String indexName, SpiMetaQueryImpl spiMetaQueryImpl, NoSqlSession session) {
		this.indexName = indexName;
		this.spiMeta = spiMetaQueryImpl;
		this.session = session;
	}

	@Override
	public void setParameter(String parameterName, ValAndType valAndType) {
		parameters.put(parameterName, valAndType);
	}

	/**
	 * Returns the primary keys as a byte[]
	 */
	@Override
	public List<byte[]> getResultList() {
		ExpressionNode root = spiMeta.getASTTree();
		
		List<byte[]> objectKeys = new ArrayList<byte[]>();
		log.info("root="+root.getExpressionAsString());
		if(root.getType() == NoSqlLexer.EQ) {
			StateAttribute attr = (StateAttribute) root.getLeftChild().getState();
			String paramName = (String) root.getRightChild().getState();
			ValAndType val = parameters.get(paramName);
			if(val == null)
				throw new IllegalStateException("You did not call setParameter for parameter= ':"+paramName+"'");
			
			String table = attr.getTableName();
			
			String index = val.getColumnMeta().getIndexPrefix();
			//if doing a partion, you can add to indexPrefix here
			//The indexPrefix is of format /<ColumnFamilyName>/<ColumnNameToIndex>/<PartitionKey>/<PartitionId> where key is like ByAccount or BySecurity and id is the id of account or security

			String columnFamily = val.getColumnMeta().getStorageType().getIndexTableName();
			byte[] rowKey = getBytes(index);
			byte[] data = val.getIndexedData();
			Iterable<Column> scan = session.columnRangeScan(columnFamily, rowKey, data, data, BATCH_SIZE);
			
			for(Column c : scan) {
				byte[] primaryKey = c.getName();
				objectKeys.add(primaryKey);
			}
			
		} else
			throw new UnsupportedOperationException("not supported yet");
		
		//session.columnRangeScan(cf, indexKey, from, to, batchSize)
		return objectKeys;
	}

	private short getLength(byte[] name) {
		ByteBuffer bb = ByteBuffer.allocate(2);
		bb.order(ByteOrder.LITTLE_ENDIAN);
		bb.put(name[name.length-2]);
		bb.put(name[name.length-1]);
		short shortVal = bb.getShort(0);
		return shortVal;
	}

	private byte[] getBytes(String index) {
		try {
			return index.getBytes("UTF8");
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException("probably a bug. could not convert into bytes from String", e);
		}
	}

	
}
