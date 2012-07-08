package com.alvazan.orm.layer3.spi.index.inmemory;

import java.util.Map;

import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Version;

import com.alvazan.orm.impl.meta.query.MetaFieldDbo;

public class LeafNode implements QueryNode {

	private QueryNode parentNode;
	
	private int comparatorType;
	
	private MetaFieldDbo field;
	
	
	private String parameterName;

	public LeafNode(QueryNode parentNode, MetaFieldDbo field,
			String parameterName, int comparatorType) {
		this.parentNode = parentNode;
		this.field = field;
		this.parameterName = parameterName;
		this.comparatorType = comparatorType;
	}
	
	public String getParameterName() {
		return parameterName;
	}

	public void setParameterName(String parameterName) {
		this.parameterName = parameterName;
	}

	public int getComparatorType() {
		return comparatorType;
	}

	public void setComparatorType(int comparatorType) {
		this.comparatorType = comparatorType;
	}

	public MetaFieldDbo getField() {
		return field;
	}

	public void setField(MetaFieldDbo field) {
		this.field = field;
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
		return "Field:[" + this.field.getName() + "] type:["
				+ this.comparatorType + "] parameter:[" + this.parameterName+"]";
	}

	@Override
	public Query getQuery(Map<String, Object> parameterValues) {
		// FIXME
		// should be KeywordAnalyzer ?
		// toString may not right.
		try {
			Object value = parameterValues.get(this.parameterName);
			Query query = new QueryParser(Version.LUCENE_36,
					this.field.getName(), new KeywordAnalyzer()).parse(value
					.toString());
			return query;
		} catch (ParseException e) {
			throw new IllegalArgumentException(e);
		}

	}

}
