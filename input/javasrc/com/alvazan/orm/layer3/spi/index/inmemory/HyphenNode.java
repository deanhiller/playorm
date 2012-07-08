package com.alvazan.orm.layer3.spi.index.inmemory;

import java.util.Map;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;

import com.alvazan.orm.parser.antlr.NoSqlLexer;

public class HyphenNode implements QueryNode{

	private QueryNode parentNode;
	private QueryNode leftNode;
	private QueryNode rightNode;
	private int type;

	public HyphenNode(QueryNode parentNode,int type) {
		this.parentNode = parentNode;
		this.type = type;
	}
	
	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

	public QueryNode getLeftNode() {
		return this.leftNode;
	}

	public void setLeftNode(QueryNode leftNode) {
		this.leftNode =  leftNode;
	}

	public QueryNode getRightNode() {
		return this.rightNode;
	}

	public void setRightNode(QueryNode rightNode) {
		this.rightNode = rightNode;
	}

	@Override
	public QueryNode getParentNode() {
		return this.parentNode;
	}

	@Override
	public void setParentNode(QueryNode parentNode) {
		this.parentNode = parentNode;
	}

	@Override
	public String toString() {
		return "Type:["+this.type+"] leftNode "+this.leftNode+" right Node:"+this.rightNode;
	}
	
	@Override
	public Query getQuery(Map<String, Object> parameterValues) {
		//check if need merge 
		if((this.leftNode instanceof LeafNode)&&(this.rightNode instanceof LeafNode)){
			//check numberic field 
			LeafNode leftNodetmp = (LeafNode)leftNode;
			LeafNode rightNodetmp = (LeafNode)rightNode;
			if(leftNodetmp.getField()==rightNodetmp.getField()){
				//TODO  need check lowerTerm and upperTerm
				Term lowerTerm = new Term(leftNodetmp.getField().getName(),
						(String) parameterValues.get(leftNodetmp
								.getParameterName()));

				Term upperTerm = new Term(rightNodetmp.getField().getName(),
						(String) parameterValues.get(rightNodetmp
								.getParameterName()));

//				RangeQuery rq1 = new RangeQuery(lowerTerm1,upperTerm1,true);
			}
		}
		
		Query left = this.leftNode.getQuery(parameterValues);
		Query right = this.rightNode.getQuery(parameterValues);
		BooleanQuery thisQuery = new BooleanQuery();
		if(NoSqlLexer.AND==this.type){
			thisQuery.add(left, Occur.MUST);
			thisQuery.add(right,Occur.MUST);
		}else{
			thisQuery.add(left, Occur.SHOULD);
			thisQuery.add(right,Occur.SHOULD);
		}
		return thisQuery;
		
	}
}
