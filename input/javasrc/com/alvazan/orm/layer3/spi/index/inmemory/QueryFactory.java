package com.alvazan.orm.layer3.spi.index.inmemory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.inject.Provider;

import org.antlr.runtime.tree.CommonTree;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.BooleanClause.Occur;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.api.spi.index.ExpressionNode;
import com.alvazan.orm.api.spi.index.SpiMetaQuery;
import com.alvazan.orm.api.spi.index.SpiQueryAdapter;
import com.alvazan.orm.api.spi.index.StateAttribute;
import com.alvazan.orm.parser.antlr.NoSqlLexer;

public class QueryFactory implements SpiMetaQuery {

	private static final Logger log = LoggerFactory.getLogger(QueryFactory.class);
	@Inject
	private Provider<SpiIndexQueryImpl> factory;
	private Indice indice;
	private ExpressionNode astTreeRoot;
	private QueryNode root;
	
	//pointer is used for setting up the tree;
	private QueryNode pointer;
	
	
	
	public void init(Indice indice) {
		this.indice = indice;
	}
	
	@Override
	public SpiQueryAdapter createQueryInstanceFromQuery(String indexName) {
		log.info("creating query for index="+indexName);
		SpiIndexQueryImpl indexQuery = factory.get();
		IndexItems index = indice.find(indexName);
		indexQuery.setup(index, this);
		return indexQuery;
	}

	
	
	@Override
	public void onHyphen(int type) {
		if(pointer==null){
			root = new HyphenNode(null,type);
			pointer = root;
			return;
		}
		if(((HyphenNode)pointer).getLeftNode()==null){
			((HyphenNode)pointer).setLeftNode(new HyphenNode(pointer,type));
			pointer = ((HyphenNode)pointer).getLeftNode();
		}else{
			((HyphenNode)pointer).setRightNode(
					new HyphenNode(pointer,type));
			pointer = ((HyphenNode)pointer).getRightNode();
		}
	}

	@Override
	public void onComparator(String parameter, String columnName,
			int type) {
		if(pointer ==null){
			root = new LeafNode(null,columnName,parameter,type);
			pointer = root;
			List<LeafNode> nodes = new ArrayList<LeafNode>();
			nodes.add((LeafNode)root);
//			this.paraNameToLeafNodes.put(parameter,nodes);
			return;
		}
		LeafNode node = new LeafNode(pointer,columnName,parameter,type); 
		if(((HyphenNode)pointer).getLeftNode()==null){
			((HyphenNode)pointer).setLeftNode(node);
			
		}else{
			((HyphenNode)pointer).setRightNode(node);
			pointer = pointer.getParentNode();
		}
//		List<LeafNode> nodes = this.paraNameToLeafNodes.get(parameter);
//		if(nodes==null){
//			nodes= new ArrayList<LeafNode>();
//			this.paraNameToLeafNodes.put(parameter, nodes);
//		}
//		nodes.add(node);
	}

	public Query getQuery(
			Map<String, Object> parameterValues) {
		//return root.getQuery(parameterValues);
		return walkTheASTTree(astTreeRoot, parameterValues);
	}

	public Query walkTheASTTree(ExpressionNode node, Map<String, Object> parameterValues) {
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
			return processAttrString(rightChild, leftChild);
		} else if(rightChild.getType() == NoSqlLexer.STR_VAL && leftChild.getType() == NoSqlLexer.ATTR_NAME) {
			return processAttrString(leftChild, rightChild);
		} else
			throw new UnsupportedOperationException("We do not support this combination yet.  lefttype="+leftChild.getType()+" righttype="+rightChild.getType());
		
		//TODO: add decimal and int.  Do we also go as far as adding "asdf" = "qwer"...seems kind of stupid to me though.
		// We do however need to add a.column1 = a.column2 in case they compare two columns
		// Do we need to add :param1 = :param2...that also seems stupid and they can do that in their own code if they want
	}

	private Query processAttrString(ExpressionNode attributeNode, ExpressionNode stringNode) {
		throw new UnsupportedOperationException("not done yet");
	}

	private Query processParamNameCombo(ExpressionNode attributeNode, ExpressionNode paramNode, Map<String, Object> parameterValues, int type) {
		Term term;
		if(type == NoSqlLexer.EQ) {
			String paramName = (String) paramNode.getState();
			StateAttribute attr = (StateAttribute) attributeNode.getState();
			String columnName = attr.getColumnName();
			String value = (String) parameterValues.get(paramName);
			term = new Term(columnName, value);
		} else
			throw new UnsupportedOperationException("not yet supported type="+type);
		
		return new TermQuery(term);
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
