package com.alvazan.orm.layer3.spi.index.inmemory;

import java.util.Map;

import javax.inject.Inject;
import javax.inject.Provider;

import org.antlr.runtime.tree.CommonTree;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.spi.index.ExpressionNode;
import com.alvazan.orm.api.spi.index.IndexReaderWriter;
import com.alvazan.orm.api.spi.index.SpiMetaQuery;
import com.alvazan.orm.api.spi.index.SpiQueryAdapter;
import com.alvazan.orm.api.spi.index.StateAttribute;
import com.alvazan.orm.parser.antlr.NoSqlLexer;

public class SpiMetaQueryImpl implements SpiMetaQuery {

	private static final Logger log = LoggerFactory.getLogger(SpiMetaQueryImpl.class);
	@Inject
	private Provider<SpiIndexQueryImpl> factory;
	private ExpressionNode astTreeRoot;
	
	
	@Override
	public SpiQueryAdapter createQueryInstanceFromQuery(String indexName) {
		log.info("creating query for index="+indexName);
		SpiIndexQueryImpl indexQuery = factory.get();
		indexQuery.setup(indexName, this);
		return indexQuery;
	}


	public Query getQuery(
			Map<String, Object> parameterValues) {
		//return root.getQuery(parameterValues);
		return walkTheASTTree(astTreeRoot, parameterValues);
	}

	public Query walkTheASTTree(ExpressionNode node, Map<String, Object> parameterValues) {
		if(node == null) {
			return new MatchAllDocsQuery(IndexReaderWriter.IDKEY);
		}
		return walkTheASTTreeImpl(node, parameterValues);
	}
	public Query walkTheASTTreeImpl(ExpressionNode node, Map<String, Object> parameterValues) {
		CommonTree expression = node.getASTNode();
		int type = node.getType();
		log.debug("where type:" + expression.getType());
		
		if(type == NoSqlLexer.AND || type == NoSqlLexer.OR) {
			return processAndOr(node, parameterValues, type);			
		}
		
		switch (type) {
		case NoSqlLexer.EQ:
		case NoSqlLexer.NE:
		case NoSqlLexer.GT:
		case NoSqlLexer.LT:
		case NoSqlLexer.GE:
		case NoSqlLexer.LE:
			return processExpression(node, parameterValues);
		default:
			throw new RuntimeException("Should never occur.  type="+type);
		}		
	}

	/**
	 * At this point, we know we have xxxxx = yyyyyyy where = could be <, >, etc. etc.  We
	 * do NOT know if these are constants parameters, etc. etc. yet.
	 * @param node
	 * @param parameterValues
	 * @return
	 */
	private Query processExpression(ExpressionNode node,
			Map<String, Object> parameterValues) {
		ExpressionNode leftChild = node.getLeftChild();
		ExpressionNode rightChild = node.getRightChild();
		
		//for now, we handle parameter with column name only.  There are other cases we need to add of course
		if(rightChild.getType() == NoSqlLexer.ATTR_NAME && leftChild.getType() == NoSqlLexer.PARAMETER_NAME) {
			return processParamNameCombo(rightChild, leftChild, parameterValues, node.getType());
		} else if(rightChild.getType() == NoSqlLexer.PARAMETER_NAME && leftChild.getType() == NoSqlLexer.ATTR_NAME) {
			return processParamNameCombo(leftChild, rightChild, parameterValues, node.getType());
		} else if(rightChild.getType() == NoSqlLexer.ATTR_NAME && leftChild.getType() == NoSqlLexer.STR_VAL) {
			return processAttrString(rightChild, leftChild, node.getType());
		} else if(rightChild.getType() == NoSqlLexer.STR_VAL && leftChild.getType() == NoSqlLexer.ATTR_NAME) {
			return processAttrString(leftChild, rightChild, node.getType());
		} else if ((rightChild.getType() == NoSqlLexer.INT_VAL || rightChild
				.getType() == NoSqlLexer.DEC_VAL)
				&& leftChild.getType() == NoSqlLexer.ATTR_NAME) {
			return processAttrNumber(leftChild, rightChild, node.getType());
		}
		else
			throw new UnsupportedOperationException("We do not support this combination yet.  lefttype="+leftChild.getType()+" righttype="+rightChild.getType());
		
		//TODO: add decimal and int.  Do we also go as far as adding "asdf" = "qwer"...seems kind of stupid to me though.
		// We do however need to add a.column1 = a.column2 in case they compare two columns
		// Do we need to add :param1 = :param2...that also seems stupid and they can do that in their own code if they want
	}

	private Query processAttrString(ExpressionNode attributeNode, ExpressionNode stringNode, int type) {
		Term term;
		if(type == NoSqlLexer.EQ) {
			//NOTE: For DECIMAL, INTEGER, we need to convert string to proper value...(shoudl validate on parser side though before we get here)
			String constantValueAsStringFromQuery = (String) stringNode.getState();
			StateAttribute attr = (StateAttribute) attributeNode.getState();
			String columnName = attr.getColumnName();
			term = new Term(columnName, constantValueAsStringFromQuery);
		} else
			throw new UnsupportedOperationException("not yet supported type="+type);
		
		return new TermQuery(term);		
	}
	
	//FIXME
	private Query processAttrNumber(ExpressionNode attributeNode, ExpressionNode numberNode, int type) {


			throw new UnsupportedOperationException("not yet supported type="+type);
		
	}

	private Query processParamNameCombo(ExpressionNode attributeNode, ExpressionNode paramNode, Map<String, Object> parameterValues, int type) {
		Term term;
		String paramName = (String) paramNode.getState();
		StateAttribute attr = (StateAttribute) attributeNode.getState();
		String columnName = attr.getColumnName();
		Object value = parameterValues.get(paramName);
		if(value==null) {
			
			term = new Term(columnName,"__impossible__value__");

			return new TermQuery(term);
		}
		switch (type) {
		case NoSqlLexer.EQ:
			NumericRangeQuery query=null;
			if(value instanceof Integer) {
				 query = NumericRangeQuery.newIntRange(columnName, (Integer)value, (Integer)value, true, true);
			}else if(value instanceof Float){
				 query = NumericRangeQuery.newFloatRange(columnName, (Float)value, (Float)value, true, true);
			}else if(value instanceof Double){
				query = NumericRangeQuery.newDoubleRange(columnName, (Double)value, (Double)value, true, true);
			}else if(value instanceof Long){
			  query = NumericRangeQuery.newLongRange(columnName, (Long)value, (Long)value, true, true);
			}
			else {
				term = new Term(columnName, value+"");
				return new TermQuery(term);
			}
			if(query!=null) return query;
		case NoSqlLexer.GT:
			return getNumbericRangeQuery(columnName, value, true, false);
		case NoSqlLexer.LT:
			return getNumbericRangeQuery(columnName, value, false, false);
		case NoSqlLexer.GE:
			return getNumbericRangeQuery(columnName, value, true, true);
		case NoSqlLexer.LE:
			return getNumbericRangeQuery(columnName,value,false,true);
		case NoSqlLexer.NE:
			throw new UnsupportedOperationException("not yet supported type="+type);
		default:
			throw new RuntimeException("Should never occur.  type="+type);
		}
		
	}
	
	private NumericRangeQuery getNumbericRangeQuery(String columnName,Object value,boolean greater,boolean include){
		if(value instanceof Integer) {
			NumericRangeQuery<Integer> query =null;
			if(greater){
				query = NumericRangeQuery.newIntRange(columnName,  (Integer)value,Integer.MAX_VALUE, true, include);	
			}else{
			   query = NumericRangeQuery.newIntRange(columnName,  Integer.MIN_VALUE,(Integer)value, true, include);	
			}
			return query;
		}else if(value instanceof Float){
			NumericRangeQuery<Float> query =null;
			if(greater){
				query = NumericRangeQuery.newFloatRange(columnName,  (Float)value,Float.MAX_VALUE, true, include);	
			}else{
			 query = NumericRangeQuery.newFloatRange(columnName,  Float.MIN_VALUE, (Float)value,  true, include);
			}
			return query;
		}else if(value instanceof Double){
			NumericRangeQuery<Double> query =null;
			if(greater){
				query = NumericRangeQuery.newDoubleRange(columnName,  (Double)value,Double.MAX_VALUE, true, include);	
			}else{
				query = NumericRangeQuery.newDoubleRange(columnName, Double.MIN_VALUE,(Double)value,  true, include);	
			}
			
			return query;
		}else if(value instanceof Long){
			NumericRangeQuery<Long> query =null;
			if(greater){
				query = NumericRangeQuery.newLongRange(columnName,  (Long)value,Long.MAX_VALUE, true, include);	
			}else{
				query = NumericRangeQuery.newLongRange(columnName, Long.MIN_VALUE,(Long)value, true, include);	
			}
			return query;
		}else {
			throw new UnsupportedOperationException("not yet supported GT for type "+value.getClass());
		}
	}
	
	
	private Query processAndOr(ExpressionNode node,
			Map<String, Object> parameterValues, int type) {
		Query left = walkTheASTTree(node.getLeftChild(), parameterValues);
		Query right = walkTheASTTree(node.getRightChild(), parameterValues);
		
		BooleanQuery thisQuery = new BooleanQuery();
		if(type == NoSqlLexer.AND){
			thisQuery.add(left, Occur.MUST);
			thisQuery.add(right,Occur.MUST);
		}else{
			thisQuery.add(left, Occur.SHOULD);
			thisQuery.add(right,Occur.SHOULD);
		}
		return thisQuery;
	}
	
	@Override
	public void setASTTree(ExpressionNode node) {
		this.astTreeRoot = node;
	}

	
}
