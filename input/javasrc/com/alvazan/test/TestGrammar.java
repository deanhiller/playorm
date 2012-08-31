package com.alvazan.test;

import org.antlr.runtime.RecognitionException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.alvazan.orm.api.base.DbTypeEnum;
import com.alvazan.orm.impl.bindings.ProductionBindings;
import com.alvazan.orm.parser.antlr.ExpressionNode;
import com.alvazan.orm.parser.antlr.InfoForWiring;
import com.alvazan.orm.parser.antlr.MetaFacade;
import com.alvazan.orm.parser.antlr.ParseQueryException;
import com.alvazan.orm.parser.antlr.ScannerSql;
import com.google.inject.Guice;
import com.google.inject.Injector;

public class TestGrammar {

	private ScannerSql scanner;
	private MetaFacade facade;
	private InfoForWiring wiring;

	@Before
	public void setup() {
		Injector injector = Guice.createInjector(new ProductionBindings(DbTypeEnum.IN_MEMORY));
		scanner = injector.getInstance(ScannerSql.class);
		wiring = new InfoForWiring("<thequery>", null);
		facade = new MockFacade();
	}
	//@Test
	public void testBetween() {
		String sql = "select p FROM MyTable as p where p.leftside between :asfd and :ff";
		ExpressionNode newTree = scanner.compileSql(sql, wiring, facade);
		String result = ""+newTree;
		Assert.assertEquals("(p.leftside between :asfd and :ff)", result);
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
	public void testSimple() {
		String sql = "select p FROM MyTable as p";
		ExpressionNode newTree = scanner.compileSql(sql, wiring, facade);
		Assert.assertNull(newTree);
	}

	@Test
	public void testOptimizeBetween2() {
		String sql = "select *  FROM TABLE as e WHERE e.numTimes >= :begin and e.numTimes < :to";
		ExpressionNode newTree = scanner.compileSql(sql, wiring, facade);
		String result = ""+newTree;
		Assert.assertEquals(":begin <= e.numTimes < :to", result);
	}
	
	@Test
	public void testOptimizeBetween() {
		String sql = "select p FROM MyTable as p where p.leftside > :asfd and p.rightside >= :ff and p.rightside < :tttt and p.leftside <= :fdfd";
		ExpressionNode newTree = scanner.compileSql(sql, wiring, facade);
		String result = ""+newTree;
		Assert.assertEquals("(:asfd < p.leftside <= :fdfd and :ff <= p.rightside < :tttt)", result);
	}
	
	@Test
	public void testRewriteJoin() {
		String sql = "select p FROM TABLE as p INNER JOIN p.security as s where p.numShares = :shares and s.securityType = :type";
		ExpressionNode newTree = scanner.compileSql(sql, wiring, facade);
		String result = ""+newTree;
		Assert.assertEquals("(p.numShares = :shares and(innerjoin) s.securityType = :type)", result);
	}
	@Test
	public void testJoinJoinOnSame() {
		String sql = "select p FROM TABLE as p INNER JOIN p.security as s INNER JOIN p.something as t where p.numShares = :shares and s.securityType = :type";
		ExpressionNode newTree = scanner.compileSql(sql, wiring, facade);
		String result = ""+newTree;
		Assert.assertEquals("(p.numShares = :shares and(innerjoin) s.securityType = :type)", result);
	}
	@Test
	public void testJoinJoinOnChain() {
		String sql = "select p FROM TABLE as p INNER JOIN p.security as s INNER JOIN s.something as t where p.numShares = :shares and s.securityType = :type";
		ExpressionNode newTree = scanner.compileSql(sql, wiring, facade);
		String result = ""+newTree;
		Assert.assertEquals("(p.numShares = :shares and(innerjoin) s.securityType = :type)", result);
	}	
	@Test
	public void testNoRewrite() {
		String sql = "select p FROM TABLE as p INNER JOIN p.security as s where p.numShares = :shares and p.something = :something";
		ExpressionNode newTree = scanner.compileSql(sql, wiring, facade);
		String result = ""+newTree;
		Assert.assertEquals("(p.numShares = :shares and p.something = :something)", result);
	}
	
	@Test
	public void testLargeTree() {
		String sql = "select a FROM TABLE as a INNER JOIN a.security as d INNER JOIN a.something as b INNER JOIN b.some as e" +
				" INNER JOIN b.two as g INNER JOIN g.two as f INNER JOIN f.try as h" +
				" where ( ((a.y>:a and d.y>:a) or (a.z>:a and b.z>:a)) and (b.x>:a and e.y>:a) )" +
				" and ( ((f.x>:a and f.y>:a) or (g.x>:a and g.y>:a)) and h.f>:a )";
		ExpressionNode newTree = scanner.compileSql(sql, wiring, facade);
		String result = ""+newTree;
		Assert.assertEquals("((((a.y > :a and(innerjoin) d.y > :a) or (a.z > :a and(innerjoin) b.z > :a)) and (b.x > :a and(innerjoin) e.y > :a)) and(innerjoin) (((f.x > :a and f.y > :a) or(innerjoin) (g.x > :a and g.y > :a)) and(innerjoin) h.f > :a))", result);
	}
}
