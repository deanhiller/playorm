package com.alvazan.test.orm.antlr.parser;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.impl.meta.MetaQueryFieldInfo;
import com.alvazan.orm.impl.meta.ScannerForQuery;
import com.alvazan.orm.layer3.spi.index.IndexReaderWriter;
import com.alvazan.orm.layer3.spi.index.inmemory.MemoryIndexWriter;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;

public class TestQueryParser implements Module {
	private static final Logger log = LoggerFactory.getLogger(TestQueryParser.class);
	
	@Test
	public void testQueryParser(){
		String sql ="select column_a from table_a where column_b=:value_b";
		
		Injector injector = Guice.createInjector(this);
		ScannerForQuery scanner = injector.getInstance(ScannerForQuery.class);
		
		Map<String, MetaQueryFieldInfo> nameToField = new HashMap<String, MetaQueryFieldInfo>();
		nameToField.put("column_a", new MockField());
		nameToField.put("column_b", new MockField());
		
		MockMeta meta = new MockMeta(nameToField);
		scanner.setup(meta, sql);
		
	}

	@Override
	public void configure(Binder binder) {
		binder.bind(IndexReaderWriter.class).to(MemoryIndexWriter.class).asEagerSingleton();
	}
	
//	@Test
//	public void testAliasQueryParser(){
//		String sql ="SelecT a.column_a,a.column_b FrOm table_a a wHerE a.column_b=:value_b";
//		QueryContext context =NoSqlTreeParser.parse(sql);
//		log.info("query context:"+context);
//		List<Attribute> projections = context.getSelectClause().getProjections();
//		Assert.assertEquals(2, projections.size());
//		Assert.assertEquals("column_a", projections.get(0).getAttributeName());
//		Assert.assertEquals("column_b", projections.get(1).getAttributeName());
//		
//		Map<Attribute, FilterParameter> filters = context.getWhereClause().getParameterMap();
//		Assert.assertEquals("column_b", filters.keySet().iterator().next().getAttributeName());
//		Assert.assertEquals("value_b", filters.values().iterator().next().getParameter());
//	}
//	
//	
//	@Test
//	public void testbbb(){
//		//String sql ="select * from Account  b where b.users >= :begin and b.users < :end";
//		String sql="select *  FROM TABLE e WHERE e.numTimes >= :begin and e.numTimes < :to and e.ttt <:too and e.bbb>=:to";
//		QueryContext context =NoSqlTreeParser.parse(sql);
//		
//		log.info("ctx="+context);
//	}
}
