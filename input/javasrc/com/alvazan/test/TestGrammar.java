package com.alvazan.test;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.junit.Assert;
import org.junit.Test;

import com.alvazan.orm.parser.antlr.NoSqlLexer;
import com.alvazan.orm.parser.antlr.NoSqlParser;
import com.alvazan.orm.parser.antlr.ParseQueryException;

public class TestGrammar {
	
	@Test
	public void testGrammar() throws RecognitionException {
		String sql = "select p FROM TABLE p yup join p.security s where p.numShares = :shares and s.securityType = :type";
		
        try {
            parseQuery(sql);        	
        	Assert.fail("should fail parsing");
        } catch(ParseQueryException e) {
        	Assert.assertEquals("yup", e.getTokenText());
        	Assert.assertEquals(22, e.getCharacterPosition());
        }
	}

	private void parseQuery(String sql) throws RecognitionException {
		ANTLRStringStream stream = new ANTLRStringStream(sql);
        NoSqlLexer lexer = new NoSqlLexer(stream);
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        NoSqlParser parser = new NoSqlParser(tokenStream);
    	parser.statement().getTree();
	}

}
