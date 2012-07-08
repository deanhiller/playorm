package com.alvazan.test.orm.antlr.parser;

import java.util.List;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.CommonTree;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.parser.antlr.NoSqlLexer;
import com.alvazan.orm.parser.antlr.NoSqlParser;

public class TestQueryParser2 {
	private static final Logger log = LoggerFactory.getLogger(TestQueryParser2.class);
	private String sql="select *  FROM TABLEA e,TABLE2,TABLE3 b WHERE e.numTimes >= \"hello\" and (e.numTimes < :to and e.ttt <'hithere') and e.bbb>=:to";

	@Test
	public void testQueryParser() throws RecognitionException{
		//String sql ="SelecT a.column_a,a.column_b FrOm table_a a wHerE a.column_b=:value_b";
		//String sql="select column_a from table_a where column_b=:value_b";
		//String sql="select *  FROM TABLEA e,TABLE2,TABLE3 b WHERE e.numTimes >= \"hello\" and e.numTimes < :to and e.ttt <'hithere' and e.bbb>=:to";
		printTree(sql);
	}

	private CommonTree printTree(String sql) throws RecognitionException {
		ANTLRStringStream stream = new ANTLRStringStream(sql);
        NoSqlLexer lexer = new NoSqlLexer(stream);
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        NoSqlParser parser = new NoSqlParser(tokenStream);	
        CommonTree tree = (CommonTree) parser.statement().getTree();
        walkStatement(tree, null, null, 0);
		return tree;
	}
	
	@SuppressWarnings("unchecked")
	public void walkStatement(CommonTree tree, Object metaVisitor, Object spiMetaVisitor, int level) {
		List<CommonTree> children = tree.getChildren();
		if(children == null)
			return;
		
		for(CommonTree child : children) {
			int type = child.getType();
			log.info(createTabs(level)+"type="+type+" text="+child.getText()+"");
			walkStatement(child, metaVisitor, spiMetaVisitor, level+1);
		}
	}
	
	public String createTabs(int count) {
		String tabs = "";
		for(int i = 0; i < count; i++) {
			tabs += "\t";
		}
		return tabs;
	}
	

}
