package com.alvazan.test;

import org.antlr.runtime.RecognitionException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.alvazan.orm.api.base.DbTypeEnum;
import com.alvazan.orm.impl.bindings.ProductionBindings;
import com.alvazan.orm.layer5.indexing.ExpressionNode;
import com.alvazan.orm.layer5.nosql.cache.InfoForWiring;
import com.alvazan.orm.layer5.nosql.cache.MetaFacade;
import com.alvazan.orm.layer5.nosql.cache.SqlScanner;
import com.alvazan.orm.parser.antlr.ParseQueryException;
import com.google.inject.Guice;
import com.google.inject.Injector;

public class TestGrammar {

	private SqlScanner scanner;
	private MetaFacade facade;
	private InfoForWiring wiring;

	@Before
	public void setup() {
		Injector injector = Guice.createInjector(new ProductionBindings(DbTypeEnum.IN_MEMORY));
		scanner = injector.getInstance(SqlScanner.class);
		wiring = new InfoForWiring("<thequery>", null);
		facade = new MockFacade();
	}
	
	@Test
	public void testGrammar() throws RecognitionException {
		String sql = "select p FROM TABLE as p yup join p.security s where p.numShares = :shares and s.securityType = :type";
		
        try {
			scanner.compileSql(sql, wiring, facade);
        	Assert.fail("should fail parsing");
        } catch(ParseQueryException e) {
        	Assert.assertEquals("yup", e.getTokenText());
        	Assert.assertEquals(25, e.getCharacterPosition());
        }
	}

	@Test
	public void testOptimizeBetween() {
		String sql = "select p FROM MyTable as p where p.leftside > :asfd and p.rightside >= :ff and p.rightside < :tttt and p.leftside <= :fdfd";
		ExpressionNode newTree = scanner.compileSql(sql, wiring, facade);
		String result = ""+newTree;
		Assert.assertEquals("(:asfd < p.leftside <= :fdfd and :ff <= p.rightside < :tttt)", result);
	}
}
