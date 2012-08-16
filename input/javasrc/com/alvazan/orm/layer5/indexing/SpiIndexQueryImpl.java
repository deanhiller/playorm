package com.alvazan.orm.layer5.indexing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.spi3.meta.DboColumnMeta;
import com.alvazan.orm.api.spi3.meta.DboTableMeta;
import com.alvazan.orm.api.spi3.meta.conv.ByteArray;
import com.alvazan.orm.api.spi5.NoSqlSession;
import com.alvazan.orm.api.spi5.SpiQueryAdapter;
import com.alvazan.orm.api.spi9.db.Column;
import com.alvazan.orm.api.spi9.db.ScanInfo;
import com.alvazan.orm.parser.antlr.NoSqlLexer;

public class SpiIndexQueryImpl implements SpiQueryAdapter {

	private static final Logger log = LoggerFactory.getLogger(SpiIndexQueryImpl.class);

	private static final int BATCH_SIZE = 500;
	
	private SpiMetaQueryImpl spiMeta;
	private NoSqlSession session;
	private Map<String, ByteArray> parameters = new HashMap<String, ByteArray>();

	private String partitionBy;
	private String partitionId;

	private Integer maxResults;
	private int firstResult;
	
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
		if(root == null) {
			DboTableMeta tableMeta = spiMeta.getMainTableMeta();
			DboColumnMeta metaCol = tableMeta.getAnyIndex();
			ScanInfo scanInfo = metaCol.createScanInfo(partitionBy, partitionId, BATCH_SIZE);
			Iterable<Column> scan = session.columnRangeScanAll(scanInfo);
			processKeys(objectKeys, scan);
		} else if(root.getType() == NoSqlLexer.EQ) {
			log.info("root="+root);
			StateAttribute attr = (StateAttribute) root.getLeftChild().getState();
			DboColumnMeta info = attr.getColumnInfo();
			
			byte[] data = retrieveValue(info, root.getRightChild());
			
			ScanInfo scanInfo = info.createScanInfo(partitionBy, partitionId, BATCH_SIZE);
			Iterable<Column> scan = session.columnRangeScan(scanInfo, data, true, data, true);
			
			processKeys(objectKeys, scan);
			
		} else if(root.getType() == NoSqlLexer.GT
				|| root.getType() == NoSqlLexer.GE
				|| root.getType() == NoSqlLexer.LT
				|| root.getType() == NoSqlLexer.LE) {
			processKeys(null, null);
			
		} else 
			throw new UnsupportedOperationException("not supported yet. type="+root.getType());
		
		return objectKeys;
	}

	private void processKeys(List<byte[]> objectKeys, Iterable<Column> scan) {
		int counter = 0;
		for(Column c : scan) {
			if(counter < firstResult)
				continue; //keep skipping until counter == firstResult
			byte[] primaryKey = c.getName();
			objectKeys.add(primaryKey);
			if(maxResults != null && objectKeys.size() >= maxResults.intValue())
				break;
		}
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

	@Override
	public void setFirstResult(int firstResult) {
		if(firstResult < 0)
			throw new IllegalArgumentException("firstResult must be 0 or greater");
		this.firstResult = firstResult;
	}

	@Override
	public void setMaxResults(int batchSize) {
		if(batchSize <= 0)
			throw new IllegalArgumentException("batchSize must be greater than 0");
		this.maxResults = batchSize;
	}
	
}
