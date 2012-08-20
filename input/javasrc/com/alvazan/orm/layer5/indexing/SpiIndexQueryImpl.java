package com.alvazan.orm.layer5.indexing;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.spi3.meta.DboColumnMeta;
import com.alvazan.orm.api.spi3.meta.DboTableMeta;
import com.alvazan.orm.api.spi3.meta.IndexColumnInfo;
import com.alvazan.orm.api.spi3.meta.conv.ByteArray;
import com.alvazan.orm.api.spi5.NoSqlSession;
import com.alvazan.orm.api.spi5.SpiQueryAdapter;
import com.alvazan.orm.api.spi9.db.IndexColumn;
import com.alvazan.orm.api.spi9.db.Key;
import com.alvazan.orm.api.spi9.db.ScanInfo;
import com.alvazan.orm.parser.antlr.NoSqlLexer;

public class SpiIndexQueryImpl implements SpiQueryAdapter {

	private static final Logger log = LoggerFactory.getLogger(SpiIndexQueryImpl.class);

	private SpiMetaQueryImpl spiMeta;
	private NoSqlSession session;
	private Map<String, ByteArray> parameters = new HashMap<String, ByteArray>();

	private String partitionBy;
	private String partitionId;
	private int batchSize = 500;
	
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

	@Override
	public Iterable<IndexColumnInfo> getResultList() {
		ExpressionNode root = spiMeta.getASTTree();
		if(root == null) {
			DboTableMeta tableMeta = spiMeta.getMainTableMeta();
			DboColumnMeta metaCol = tableMeta.getAnyIndex();
			ScanInfo scanInfo = metaCol.createScanInfo(partitionBy, partitionId);
			Iterable<IndexColumn> scan = session.scanIndex(scanInfo, null, null, batchSize);
			return processKeys(null, scan);
		}
	
		log.info("root(type="+root.getType()+")="+root);
		
		return processExpressionTree(root);
	}

	private Iterable<IndexColumnInfo> processExpressionTree(ExpressionNode parent) {
		int type = parent.getType();
		switch (type) {
		case NoSqlLexer.AND:
		case NoSqlLexer.OR:
			return processAndOr(parent);
		case NoSqlLexer.EQ:
		case NoSqlLexer.NE:
		case NoSqlLexer.GT:
		case NoSqlLexer.LT:
		case NoSqlLexer.GE:
		case NoSqlLexer.LE:
			return processRangeExpression(parent);
		default:
			throw new UnsupportedOperationException("bug, unsupported type="+type);
		}
	}

	private Iterable<IndexColumnInfo> processAndOr(ExpressionNode root) {
		ExpressionNode left = root.getChild(ChildSide.LEFT);
		ExpressionNode right = root.getChild(ChildSide.RIGHT);
		
		Iterable<IndexColumnInfo> leftResults = processExpressionTree(left);
		Iterable<IndexColumnInfo> rightResults = processExpressionTree(right);
		
		if(root.getType() == NoSqlLexer.AND) {
			return new IterableForAnd(leftResults, rightResults);
		} else {
			return new IterableForOr(leftResults, rightResults);
		}
	}
	
	private Iterable<IndexColumnInfo> processRangeExpression(ExpressionNode root) {
		StateAttribute attr = (StateAttribute) root.getLeftChild().getState();
		DboColumnMeta info = attr.getColumnInfo();
		ScanInfo scanInfo = info.createScanInfo(partitionBy, partitionId);
		
		Iterable<IndexColumn> scan;
		if(root.getType() == NoSqlLexer.EQ) {
			byte[] data = retrieveValue(info, root.getRightChild());
			Key key = new Key(data, true);
			scan = session.scanIndex(scanInfo, key, key, batchSize);
		} else if(root.getType() == NoSqlLexer.GT
				|| root.getType() == NoSqlLexer.GE
				|| root.getType() == NoSqlLexer.LT
				|| root.getType() == NoSqlLexer.LE) {
			Key from = null;
			Key to = null;
			if(root.isInBetweenExpression()) {
				ExpressionNode node = root.getGreaterThan();
				ExpressionNode node2 = root.getLessThan();
				from = createLeftKey(node, info);
				to = createRightKey(node2, info);
			} else if(root.getType() == NoSqlLexer.GT
					|| root.getType() == NoSqlLexer.GE) {
				from = createLeftKey(root, info);
			} else if(root.getType() == NoSqlLexer.LT) {
				to = createRightKey(root, info);
			} else
				throw new UnsupportedOperationException("not done yet here");
			
			scan = session.scanIndex(scanInfo, from, to, batchSize);
			
		} else 
			throw new UnsupportedOperationException("not supported yet. type="+root.getType());

		Iterable<IndexColumnInfo> processKeys = processKeys(info, scan);
		return processKeys;
	}

	private Key createRightKey(ExpressionNode node, DboColumnMeta info) {
		byte[] data = retrieveValue(info, node.getRightChild());
		if(node.getType() == NoSqlLexer.LT)
			return new Key(data, false);
		else if(node.getType() == NoSqlLexer.LE)
			return new Key(data, true);
		else
			throw new RuntimeException("bug, should never happen, but should be easy to fix this one");
	}

	private Key createLeftKey(ExpressionNode node, DboColumnMeta info) {
		byte[] data = retrieveValue(info, node.getRightChild());
		if(node.getType() == NoSqlLexer.GT)
			return new Key(data, false);
		else if(node.getType() == NoSqlLexer.GE)
			return new Key(data, true);
		else
			throw new RuntimeException("bug, should never happen, but should be easy to fix this one. type="+node.getType());	
	}

	private Iterable<IndexColumnInfo> processKeys(DboColumnMeta info, Iterable<IndexColumn> scan) {
		return new SpiIterProxy(info, scan);
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
	public void setBatchSize(int batchSize) {
		if(batchSize <= 0)
			throw new IllegalArgumentException("batchSize must be 1 or greater, but really, please don't use 1, use something like 500(the default anyways)");
		this.batchSize = batchSize;
	}
	
}
