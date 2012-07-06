package com.alvazan.test.orm.antlr.parser;

import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.parser.NoSqlTreeParser;
import com.alvazan.orm.parser.QueryContext;
import com.alvazan.orm.parser.tree.Attribute;
import com.alvazan.orm.parser.tree.FilterParameter;

public class TestQueryParser {
	private static final Logger log = LoggerFactory.getLogger(TestQueryParser.class);
	
	@Test
	public void testQueryParser(){
		String sql ="select column_a from table_a where column_b=:value_b";
		QueryContext context =NoSqlTreeParser.parse(sql);
		log.info("query context:"+context);
		List<Attribute> projections = context.getSelectClause().getProjections();
		Assert.assertEquals(1, projections.size());
		Assert.assertEquals("column_a", projections.get(0).getAttributeName());
		
		List<String> entities = context.getFromClause().getEntities();
		Assert.assertEquals(1, entities.size());
		Assert.assertEquals("table_a", entities.get(0));
		
	}
	
	@Test
	public void testAliasQueryParser(){
		String sql ="SelecT a.column_a,a.column_b FrOm table_a a wHerE a.column_b=:value_b";
		QueryContext context =NoSqlTreeParser.parse(sql);
		log.info("query context:"+context);
		List<Attribute> projections = context.getSelectClause().getProjections();
		Assert.assertEquals(2, projections.size());
		Assert.assertEquals("column_a", projections.get(0).getAttributeName());
		Assert.assertEquals("column_b", projections.get(1).getAttributeName());
		
		Map<Attribute, FilterParameter> filters = context.getWhereClause().getParameterMap();
		Assert.assertEquals("column_b", filters.keySet().iterator().next().getAttributeName());
		Assert.assertEquals("value_b", filters.values().iterator().next().getParameter());
	}
	
	
	@Test
	public void testbbb(){
		//String sql ="select * from Account  b where b.users >= :begin and b.users < :end";
		String sql="select *  FROM TABLE e WHERE e.numTimes >= :begin and e.numTimes < :to and e.ttt <:too and e.bbb>=:to";
		QueryContext context =NoSqlTreeParser.parse(sql);
		
		log.info("ctx="+context);
	}
}
