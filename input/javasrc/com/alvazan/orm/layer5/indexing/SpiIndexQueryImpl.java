package com.alvazan.orm.layer5.indexing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.spi3.meta.DboColumnMeta;
import com.alvazan.orm.api.spi3.meta.conv.ByteArray;
import com.alvazan.orm.api.spi3.meta.conv.StandardConverters;
import com.alvazan.orm.api.spi5.NoSqlSession;
import com.alvazan.orm.api.spi5.SpiQueryAdapter;
import com.alvazan.orm.api.spi9.db.Column;
import com.alvazan.orm.api.spi9.db.ScanInfo;
import com.alvazan.orm.parser.antlr.NoSqlLexer;

public class SpiIndexQueryImpl implements SpiQueryAdapter {

	private static final Logger log = LoggerFactory.getLogger(SpiIndexQueryImpl.class);

	private static final int BATCH_SIZE = 2000;
	
	private SpiMetaQueryImpl spiMeta;
	private NoSqlSession session;
	private Map<String, ByteArray> parameters = new HashMap<String, ByteArray>();

	private String partitionBy;
	private String partitionId;
	
	public void setup(String partitionBy, String partitionId, SpiMetaQueryImpl spiMetaQueryImpl, NoSqlSession session) {
		this.partitionBy = partitionBy;
		this.partitionId = partitionId;
		this.spiMeta = spiMetaQueryImpl;
		this.session = session;
	}

	@Override
	public void setParameter(String parameterName, byte[] value) {
		ByteArray val = new ByteArray(value);
		parameters.put(parameterName, val);
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
			DboColumnMeta info = attr.getColumnInfo();
			
			byte[] data = retrieveValue(info, root.getRightChild());
			
			String realColFamily = info.getOwner().getColumnFamily();
			String colName = info.getColumnName();
			
			String columnFamily = info.getIndexTableName();
			String indexRowKey = info.getIndexRowKey(partitionBy, partitionId);
			//if doing a partion, you can add to indexPrefix here
			//The indexPrefix is of format /<ColumnFamilyName>/<ColumnNameToIndex>/<PartitionKey>/<PartitionId> where key is like ByAccount or BySecurity and id is the id of account or security
			byte[] rowKey = StandardConverters.convertToBytes(indexRowKey);
			ScanInfo scanInfo = new ScanInfo(realColFamily, colName, columnFamily, rowKey);
			Iterable<Column> scan = session.columnRangeScan(scanInfo, data, data, BATCH_SIZE);
			
			for(Column c : scan) {
				byte[] primaryKey = c.getName();
				objectKeys.add(primaryKey);
			}
			
		} else
			throw new UnsupportedOperationException("not supported yet");
		
		//session.columnRangeScan(cf, indexKey, from, to, batchSize)
		return objectKeys;
	}

	private byte[] retrieveValue(DboColumnMeta info, ExpressionNode node) {
		if(node.getType() == NoSqlLexer.PARAMETER_NAME) {
			return processParam(info, node);
		} else if(node.getType() == NoSqlLexer.DECIMAL || node.getType() == NoSqlLexer.STR_VAL
				|| node.getType() == NoSqlLexer.INT_VAL) {
			return processConstant(info, node);
		} else 
			throw new UnsupportedOperationException("type not supported="+node.getType());
	}

	private byte[] processConstant(DboColumnMeta info, ExpressionNode node) {
		//constant is either BigDecimal, BigInteger or a String
		Object constant = node.getState();
		return info.convertToStorage2(constant);
	}

	private byte[] processParam(DboColumnMeta info, ExpressionNode node) {
		String paramName = (String) node.getState();
		ByteArray val = parameters.get(paramName);
		if(val == null)
			throw new IllegalStateException("You did not call setParameter for parameter= ':"+paramName+"'");

		byte[] data = val.getKey();
		return data;
	}

	
}
