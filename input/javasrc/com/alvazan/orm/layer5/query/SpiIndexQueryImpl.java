package com.alvazan.orm.layer5.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


import com.alvazan.orm.api.z5api.IndexColumnInfo;
import com.alvazan.orm.api.z5api.NoSqlSession;
import com.alvazan.orm.api.z5api.SpiQueryAdapter;
import com.alvazan.orm.api.z8spi.Key;
import com.alvazan.orm.api.z8spi.KeyValue;
import com.alvazan.orm.api.z8spi.Row;
import com.alvazan.orm.api.z8spi.ScanInfo;
import com.alvazan.orm.api.z8spi.action.IndexColumn;
import com.alvazan.orm.api.z8spi.conv.ByteArray;
import com.alvazan.orm.api.z8spi.iter.AbstractCursor;
import com.alvazan.orm.api.z8spi.iter.DirectCursor;
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
	public DirectCursor<IndexColumnInfo> getResultList(Set<ViewInfo> alreadyJoinedViews) {
		if(alreadyJoinedViews == null || alreadyJoinedViews.size() != 0)
			throw new IllegalArgumentException("You must pass us a non-null Set that is EMPTY and not null");
		DirectCursor<IndexColumnInfo> cursor = getResultListImpl(alreadyJoinedViews);
		return cursor;
	}
	
	public DirectCursor<IndexColumnInfo> getResultListImpl(Set<ViewInfo> alreadyJoinedViews) {
		ExpressionNode root = spiMeta.getASTTree();
		if(root == null) {
			ViewInfoImpl tableInfo = (ViewInfoImpl) spiMeta.getTargetViews().get(0);
			DboTableMeta tableMeta = tableInfo.getTableMeta();
			DboColumnMeta metaCol = tableMeta.getAnyIndex();
			ScanInfo scanInfo = createScanInfo(tableInfo, metaCol);

			alreadyJoinedViews.add(tableInfo);
			AbstractCursor<IndexColumn> scan = session.scanIndex(scanInfo, null, null, batchSize);
			return processKeys(tableInfo, null, scan);
		}
	
		return processExpressionTree(root, alreadyJoinedViews);
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

	private DirectCursor<IndexColumnInfo> processExpressionTree(ExpressionNode parent, Set<ViewInfo> alreadyJoinedViews) {
		int type = parent.getType();
		switch (type) {
		case NoSqlLexer.AND:
		case NoSqlLexer.OR:
			return processAndOr(parent, alreadyJoinedViews);
		case NoSqlLexer.EQ:
		case NoSqlLexer.NE:
		case NoSqlLexer.GT:
		case NoSqlLexer.LT:
		case NoSqlLexer.GE:
		case NoSqlLexer.LE:
		case NoSqlLexer.BETWEEN:
			return processRangeExpression(parent, alreadyJoinedViews);
		default:
			throw new UnsupportedOperationException("bug, unsupported type="+type);
		}
	}

	private DirectCursor<IndexColumnInfo> processAndOr(ExpressionNode root, Set<ViewInfo> alreadyJoinedViews) {
		ExpressionNode left = root.getChild(ChildSide.LEFT);
		ExpressionNode right = root.getChild(ChildSide.RIGHT);
		
		DirectCursor<IndexColumnInfo> leftResults = processExpressionTree(left, alreadyJoinedViews);
		DirectCursor<IndexColumnInfo> rightResults = processExpressionTree(right, alreadyJoinedViews);
		
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
			temp.setColMeta(col);
			temp.setScanInfo(scanInfo);
			temp.setSession(session);
			temp.setBatchSize(batchSize);
			leftResults = temp;
			leftView = newView;
		}
		
		if(root.getType() == NoSqlLexer.AND) {
			CursorForAnd cursor = new CursorForAnd(leftView, leftResults, rightView, rightResults);
			//AND always returns LESS results(or same) than the left or right sides, 
			//sooooo, we cache results if there is less than 500 results
			return new CachingCursor<IndexColumnInfo>(cursor);
		} else {
			//Since OR always returns MORE results(or the same) as the left or right views
			//There is no need to use a caching cursor as the people below us have a caching
			//cursor AND there would be no performance benefit
			return new CursorForOr(leftView, leftResults, rightView, rightResults);
		}
	}
	
	private DirectCursor<IndexColumnInfo> processRangeExpression(ExpressionNode root, Set<ViewInfo> alreadyJoinedViews) {
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
		alreadyJoinedViews.add(viewInfo);
		
		if(info.isIndexed()) {
			//its an indexed column
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
			DirectCursor<IndexColumnInfo> processKeys = processKeys(viewInfo, info, scan);
			return processKeys;
		} else if (info.getOwner().getIdColumnMeta().getColumnName().equals(info.getColumnName())) {
			//its a non-indexed primary key
			AbstractCursor<KeyValue<Row>> scan;
			if(root.getType() == NoSqlLexer.EQ) {
				byte[] data = retrieveValue(info, root.getChild(ChildSide.RIGHT));
				byte[] virtualkey = info.getOwner().getIdColumnMeta().formVirtRowKey(data);
				List<byte[]> keyList= new ArrayList<byte[]>();
				keyList.add(virtualkey);
				scan = session.find(info.getOwner(), keyList, false, true, batchSize);
			} else
				throw new UnsupportedOperationException("Other operations not supported yet for Primary Key. Use @NoSQLIndexed for Primary Key.type="+root.getType());
			DirectCursor<IndexColumnInfo> processKeys = processKeysforPK(viewInfo, info, scan);
			return processKeys;
		} else
			throw new IllegalArgumentException("You cannot have '"+info.getColumnName() + "' in your sql query since "+info.getColumnName()+" is neither a Primary Key nor a column with @Index annotation on the field in the entity");			
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

	private DirectCursor<IndexColumnInfo> processKeys(ViewInfo viewInfo, DboColumnMeta info, AbstractCursor<IndexColumn> scan) {
		DirectCursor<IndexColumnInfo> cursor = new CursorSimpleTranslator(viewInfo, info, scan);
		return new CachingCursor<IndexColumnInfo>(cursor);
	}

	private DirectCursor<IndexColumnInfo> processKeysforPK(ViewInfo viewInfo, DboColumnMeta info, AbstractCursor<KeyValue<Row>> scan) {
		DirectCursor<IndexColumnInfo> cursor = new CursorForPrimaryKey(viewInfo, info, scan);
		return new CachingCursor<IndexColumnInfo>(cursor);
	}

	private byte[] retrieveValue(DboColumnMeta info, ExpressionNode node) {
		if(node.isParameter()) {
			return processParam(info, node);
		} else if(node.isConstant()) {
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
