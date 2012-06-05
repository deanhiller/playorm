package com.alvazan.test.orm.antlr.parser;

import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.parser.NoSqlTreeParser;
import com.alvazan.orm.parser.QueryContext;

public class TestQueryParser {
	private static final Logger log = LoggerFactory.getLogger(TestQueryParser.class);

	
	@Test
	public void testEmpty() {
		log.info("empty so test passes for now");
	}
	
	@Test
	public void testQueryParser(){
		String sql ="select column_a from table_a where column_b=:value_b";
		QueryContext context =NoSqlTreeParser.parse(sql);
		log.info("query context:"+context);
		List<String> projects = context.getSelectClause().getProjections();
		Assert.assertEquals(1, projects.size());
		Assert.assertEquals("column_a", projects.get(0));
		
		List<String> entities = context.getFromClause().getEntities();
		Assert.assertEquals(1, entities.size());
		Assert.assertEquals("table_a", entities.get(0));
		
	}
}
