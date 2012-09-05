package com.alvazan.orm.layer5.query;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.z5api.IndexColumnInfo;
import com.alvazan.orm.api.z5api.NoSqlSession;
import com.alvazan.orm.api.z5api.SpiQueryAdapter;
import com.alvazan.orm.api.z8spi.Key;
import com.alvazan.orm.api.z8spi.ScanInfo;
import com.alvazan.orm.api.z8spi.action.IndexColumn;
import com.alvazan.orm.api.z8spi.conv.ByteArray;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;
import com.alvazan.orm.api.z8spi.meta.DboColumnMeta;
import com.alvazan.orm.api.z8spi.meta.DboTableMeta;
import com.alvazan.orm.api.z8spi.meta.ViewInfo;
import com.alvazan.orm.parser.antlr.ChildSide;
import com.alvazan.orm.parser.antlr.ExpressionNode;
import com.alvazan.orm.parser.antlr.JoinInfo;
import com.alvazan.orm.parser.antlr.JoinMeta;
import com.alvazan.orm.parser.antlr.JoinType;
import com.alvazan.orm.parser.antlr.NoSqlLexer;
import com.alvazan.orm.parser.antlr.PartitionMeta;
import com.alvazan.orm.parser.antlr.StateAttribute;
import com.alvazan.orm.parser.antlr.ViewInfoImpl;

public class SpiIndexQueryImpl implements SpiQueryAdapter {

	private static final Logger log = LoggerFactory.getLogger(SpiIndexQueryImpl.class);

	private SpiMetaQueryImpl spiMeta;
	private NoSqlSession session;
	private Map<String, ByteArray> parameters = new HashMap<String, ByteArray>();

	private Integer batchSize = null;
	
	public void setup(SpiMetaQueryImpl spiMetaQueryImpl, NoSqlSession session) {
		this.spiMeta = spiMetaQueryImpl;
		this.session = session;
	}

	@Override
	public void setParameter(String parameterName, byte[] value) {
		ByteArray val = new ByteArray(value);
		parameters.put(parameterName, val);
	}

	private ByteArray getParameter(String parameterName) {
		ByteArray result = parameters.get(parameterName);
		if(result == null)
			throw new IllegalStateException("You did not call query.setParameter(\""+parameterName+"\", <yourvalue>) and that parameter is required");
		return result;
	}
	
	@Override
	public AbstractCursor<IndexColumnInfo> getResultList() {
		ExpressionNode root = spiMeta.getASTTree();
		if(root == null) {
			ViewInfoImpl tableInfo = (ViewInfoImpl) spiMeta.getMainViewMeta();
			DboTableMeta tableMeta = tableInfo.getTableMeta();
			DboColumnMeta metaCol = tableMeta.getAnyIndex();
			ScanInfo scanInfo = createScanInfo(tableInfo, metaCol);
			
			AbstractCursor<IndexColumn> scan = session.scanIndex(scanInfo, null, null, batchSize);
			return processKeys(tableInfo, null, scan);
		}
	
		log.info("root(type="+root.getType()+")="+root);
		
		return processExpressionTree(root);
	}

	private ScanInfo createScanInfo(ViewInfoImpl tableInfo, DboColumnMeta metaCol) {
		PartitionMeta partitionMeta = tableInfo.getPartition();
		String partitionBy = null;
		String partitionId = null;
		if(partitionMeta != null) {
			DboColumnMeta colMeta = partitionMeta.getPartitionColumn();
			partitionBy = colMeta.getColumnName();
			byte[] partId = retrieveValue(colMeta, (ExpressionNode) partitionMeta.getNode());
			Object partIdObj = colMeta.convertFromStorage2(partId);
			partitionId = colMeta.convertTypeToString(partIdObj);
		}
		

		ScanInfo scanInfo = ScanInfo.createScanInfo(metaCol, partitionBy, partitionId);
		return scanInfo;
	}

	private AbstractCursor<IndexColumnInfo> processExpressionTree(ExpressionNode parent) {
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
		case NoSqlLexer.BETWEEN:
			return processRangeExpression(parent);
		default:
			throw new UnsupportedOperationException("bug, unsupported type="+type);
		}
	}

	private AbstractCursor<IndexColumnInfo> processAndOr(ExpressionNode root) {
		ExpressionNode left = root.getChild(ChildSide.LEFT);
		ExpressionNode right = root.getChild(ChildSide.RIGHT);
		
		AbstractCursor<IndexColumnInfo> leftResults = processExpressionTree(left);
		AbstractCursor<IndexColumnInfo> rightResults = processExpressionTree(right);
		
		JoinMeta joinMeta = left.getJoinMeta();
		ViewInfo leftView = joinMeta.getPrimaryJoinInfo().getPrimaryTable();
		JoinMeta joinMeta2 = right.getJoinMeta();
		ViewInfo rightView = joinMeta2.getPrimaryJoinInfo().getPrimaryTable();
		
		JoinType joinType = root.getJoinMeta().getJoinType();
		if(joinType == JoinType.INNER || joinType == JoinType.LEFT_OUTER) {
			//We need to proxy the right results to translate to the same primary key as the
			//left results and our And and Or Cursor can then take care of the rest
			JoinInfo joinInfo = root.getJoinMeta().getPrimaryJoinInfo();
			ViewInfoImpl newView = joinInfo.getPrimaryTable();
			DboColumnMeta col = joinInfo.getPrimaryCol();
			ScanInfo scanInfo = createScanInfo(newView, col);
			//FROM an ORM perspective, we join to smaller tables in general as we don't want to blow out memory so do the
			//join first(ie. we process left sides first in and and or cursors)
			CursorForJoin temp = new CursorForJoin(newView, leftView, leftResults, joinType);
			temp.setScanInfo(scanInfo);
			temp.setSession(session);
			temp.setBatchSize(batchSize);
			leftResults = temp;
			leftView = newView;
		}
		
		if(root.getType() == NoSqlLexer.AND) {
			return new CursorForAnd(leftView, leftResults, rightView, rightResults);
		} else {
			return new CursorForOr(leftView, leftResults, rightView, rightResults);
		}
	}
	
	private AbstractCursor<IndexColumnInfo> processRangeExpression(ExpressionNode root) {
		StateAttribute attr;
		if(root.getType() == NoSqlLexer.BETWEEN) {
			ExpressionNode grandChild = root.getChild(ChildSide.LEFT).getChild(ChildSide.LEFT);
			attr = (StateAttribute) grandChild.getState();
		} else {
			attr = (StateAttribute) root.getChild(ChildSide.LEFT).getState();
		}
		
		DboColumnMeta info = attr.getColumnInfo();
		ViewInfoImpl viewInfo = attr.getViewInfo();		
		ScanInfo scanInfo = createScanInfo(viewInfo, info);
		
		AbstractCursor<IndexColumn> scan;
		if(root.getType() == NoSqlLexer.EQ) {
			byte[] data = retrieveValue(info, root.getChild(ChildSide.RIGHT));
			Key key = new Key(data, true);
			scan = session.scanIndex(scanInfo, key, key, batchSize);
		} else if(root.getType() == NoSqlLexer.GT
				|| root.getType() == NoSqlLexer.GE
				|| root.getType() == NoSqlLexer.LT
				|| root.getType() == NoSqlLexer.LE
				|| root.isInBetweenExpression()) {
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

		AbstractCursor<IndexColumnInfo> processKeys = processKeys(viewInfo, info, scan);
		return processKeys;
	}

	private Key createRightKey(ExpressionNode node, DboColumnMeta info) {
		byte[] data = retrieveValue(info, node.getChild(ChildSide.RIGHT));
		if(node.getType() == NoSqlLexer.LT)
			return new Key(data, false);
		else if(node.getType() == NoSqlLexer.LE)
			return new Key(data, true);
		else
			throw new RuntimeException("bug, should never happen, but should be easy to fix this one");
	}

	private Key createLeftKey(ExpressionNode node, DboColumnMeta info) {
		byte[] data = retrieveValue(info, node.getChild(ChildSide.RIGHT));
		if(node.getType() == NoSqlLexer.GT)
			return new Key(data, false);
		else if(node.getType() == NoSqlLexer.GE)
			return new Key(data, true);
		else
			throw new RuntimeException("bug, should never happen, but should be easy to fix this one. type="+node.getType());	
	}

	private AbstractCursor<IndexColumnInfo> processKeys(ViewInfo viewInfo, DboColumnMeta info, AbstractCursor<IndexColumn> scan) {
		return new CursorSimpleTranslator(viewInfo, info, scan);
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
		ByteArray val = getParameter(paramName);
		return val.getKey();
	}

	@Override
	public void setBatchSize(int batchSize) {
		if(batchSize <= 0)
			throw new IllegalArgumentException("batchSize must be 1 or greater, but really, please don't use 1, use something like 500(the default anyways)");
		this.batchSize = batchSize;
	}
	
}
