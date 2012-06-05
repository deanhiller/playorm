package com.alvazan.orm.parser;
import java.util.List;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.CommonTree;

import com.alvazan.orm.parser.antlr.NoSqlLexer;
import com.alvazan.orm.parser.antlr.NoSqlParser;
import com.alvazan.orm.parser.tree.FromClause;
import com.alvazan.orm.parser.tree.SelectClause;
import com.alvazan.orm.parser.tree.WhereClause;


/**
 * 
 *
 * @author Huai Jiang
 */
public class NoSqlTreeParser {

    public static QueryContext parse(String query) {
    	CommonTree tree = parseTree(query);
    	QueryContext context = new QueryContext();
    	parse(tree,context);
       return context; 
    }

    private static CommonTree parseTree(String query) {
        ANTLRStringStream stream = new ANTLRStringStream(query);
        NoSqlLexer lexer = new NoSqlLexer(stream);
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        NoSqlParser parser = new NoSqlParser(tokenStream);

        try {
            return (CommonTree) parser.selectStatement().getTree();
        } catch (RecognitionException re) {
            throw new RuntimeException(query, re);
        }
    }

    private static void parse(CommonTree tree, QueryContext context) {
        switch (tree.getType()) {
            case NoSqlLexer.SELECT_CLAUSE:
            	SelectClause select= parseSelectClause(tree);
            	context.setSelectClause(select);
            	break;
            case NoSqlLexer.FROM_CLAUSE:
            	FromClause from= parseFromClause(tree);
            	context.setFromClause(from);
            	break;
            case NoSqlLexer.WHERE_CLAUSE:
                WhereClause where= parseWhereClause(tree);
                context.setWhereClause(where);
                break;
      
            case 0: //nil
                List<CommonTree> childrenList = tree.getChildren();
                for (CommonTree child : childrenList) {
                    	parse(child,context);
                }
                break;
            default:
                break;
        }
        
    }

    
    private static WhereClause parseWhereClause(CommonTree tree) {
    	//TODO
    	return null;
	}

	private static FromClause parseFromClause(CommonTree tree) {
		List<CommonTree> childrenList = tree.getChildren();
		FromClause from = new FromClause();
		 if (childrenList != null && childrenList.size() > 0) {
	            for (CommonTree child : childrenList) {
	                switch (child.getType()) {
	                    case NoSqlLexer.TABLE_NAME:
	                    	from.addEntity(child.getChild(0).getText());
	                    	break;
	                    default:
	                        break;
	                }
	            }
	        }
		return from;
	}

	private static SelectClause parseSelectClause(CommonTree tree) {

		List<CommonTree> childrenList = tree.getChildren();
		SelectClause select = new SelectClause();
		 if (childrenList != null && childrenList.size() > 0) {
	            for (CommonTree child : childrenList) {
	                switch (child.getType()) {
	                	case NoSqlLexer.RESULT:
	                		return parseResult(child);
	                		
	                    default:
	                        break;
	                }
	            }
	        }
		return select;
	}

	private static SelectClause parseResult(CommonTree tree) {
		List<CommonTree> childrenList = tree.getChildren();
		SelectClause select = new SelectClause();
		 if (childrenList != null && childrenList.size() > 0) {
	            for (CommonTree child : childrenList) {
	                switch (child.getType()) {
	                    case NoSqlLexer.STAR:
	                    	select.addProjection(child.getText());
	                    	break;
	                    case NoSqlLexer.ATTR_NAME:
	                    	select.addProjection(child.getChild(0).getText());
	                        break;
	                    default:
	                        break;
	                }
	            }
	        }
		return select;
		
	}



}
