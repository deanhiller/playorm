package com.alvazan.orm.parser;
import java.util.List;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.CommonTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alvazan.orm.parser.antlr.NoSqlLexer;
import com.alvazan.orm.parser.antlr.NoSqlParser;
import com.alvazan.orm.parser.tree.FilterAttribute;
import com.alvazan.orm.parser.tree.FilterExpression;
import com.alvazan.orm.parser.tree.FilterParameter;
import com.alvazan.orm.parser.tree.FromClause;
import com.alvazan.orm.parser.tree.SelectClause;
import com.alvazan.orm.parser.tree.WhereClause;


/**
 * 
 *
 * @author Huai Jiang
 */
public class NoSqlTreeParser {
	
	
	private static final Logger log = LoggerFactory.getLogger(NoSqlTreeParser.class);
	

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

    
    //TODO not finish yet
    private static WhereClause parseWhereClause(CommonTree tree) {
    	List<CommonTree> childrenList = tree.getChildren();
    	WhereClause where = new WhereClause();
    	 if (childrenList != null && childrenList.size() > 0) {
	            for (CommonTree child : childrenList) {
	                switch (child.getType()) {
	                    case NoSqlLexer.AND:
	                    case NoSqlLexer.OR:
	                    	log.info("where type:"+child.getType());
	                    	WhereClause subExpression = parseWhereClause(child);
	                    	
	                    	break;
	                    case NoSqlLexer.EQ:
	                    case NoSqlLexer.NE:
	                    case NoSqlLexer.GT:
	                    case NoSqlLexer.LT:
	                    case NoSqlLexer.GE:
	                    case NoSqlLexer.LE:
	                    	log.info("where type:"+child.getType());
	                    	FilterAttribute attribute = new FilterAttribute();
	                    	attribute.setAttributeName(child.getChild(0).getText());
	                    	FilterParameter parameter = new FilterParameter();
	                    	parameter.setParameter(child.getChild(1).getChild(0).getText());
	                    	FilterExpression expression = new FilterExpression();
	                    	expression.setLeftNode(attribute);
	                    	expression.setRightNode(parameter);
	                    	where.setExpression(expression);
	                    	where.getParameterMap().put("key", parameter.getParameter());
	                    	break;
	                    default:
	                        break;
	                }
	            }
	        }
		return where;
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
