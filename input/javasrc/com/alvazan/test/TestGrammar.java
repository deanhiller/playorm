package com.alvazan.test;

import org.antlr.runtime.RecognitionException;
import org.junit.Assert;
import org.junit.Test;

import com.alvazan.orm.layer5.nosql.cache.ScannerForQuery;
import com.alvazan.orm.parser.antlr.ParseQueryException;

public class TestGrammar {
	
	@Test
	public void testGrammar() throws RecognitionException {
		String sql = "select p FROM TABLE as p yup join p.security s where p.numShares = :shares and s.securityType = :type";
		
        try {
        	ScannerForQuery.parseTree(sql);
        	Assert.fail("should fail parsing");
        } catch(ParseQueryException e) {
        	Assert.assertEquals("yup", e.getTokenText());
        	Assert.assertEquals(25, e.getCharacterPosition());
        }
	}

}
