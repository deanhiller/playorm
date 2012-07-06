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
import com.alvazan.orm.parser.tree.Attribute;
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
    	context.verifyAndUpdateEntity();
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

    @SuppressWarnings("unchecked")
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
    
    @SuppressWarnings("unchecked")
	private static WhereClause parseWhereClause(CommonTree tree) {
    	List<CommonTree> childrenList = tree.getChildren();
    	WhereClause where = new WhereClause();
    	 if (childrenList != null && childrenList.size() > 0) {
    		 	String aliasEntity=FromClause.DEFAULTENTITY;
	            for (CommonTree child : childrenList) {
	                switch (child.getType()) {
	                    case NoSqlLexer.AND:
	                    case NoSqlLexer.OR:
	                    	log.info("where type:"+child.getType());
	                    	WhereClause subExpression = parseWhereClause((CommonTree) child);
	                    	for(Attribute attr:subExpression.getParameterMap().keySet()){
	                    		where.getParameterMap().put(attr, subExpression.getParameterMap().get(attr));
	                    	}
	                    	where.setExpression(subExpression.getExpression());
	                    	
	                    	break;
	                    case NoSqlLexer.EQ:
	                    case NoSqlLexer.NE:
	                    case NoSqlLexer.GT:
	                    case NoSqlLexer.LT:
	                    case NoSqlLexer.GE:
	                    case NoSqlLexer.LE:
	                    	log.info("where type:"+child.getType());
	                    	int start =0;
	                    	if(child.getChild(0).getType()==NoSqlLexer.ALIAS){
	                    		aliasEntity = child.getChild(0).getText();
	                    		start=1;
	                    	}
	                    	Attribute attribute = new Attribute(aliasEntity,child.getChild(start).getText());
	                    	FilterParameter parameter = new FilterParameter();
	                    	parameter.setParameter(child.getChild(start+1).getText());
	                    	FilterExpression expression = new FilterExpression();
	                    	expression.setLeftNode(attribute);
//	                    	expression.setHyphen(hyphen)
	                    	expression.setRightNode(parameter);
	                    	if(where.getExpression()==null)
	                    		where.setExpression(expression);
	                    	else{
	                    		//tree down 
	                    		FilterExpression newExpression= new FilterExpression();
	                    		newExpression.setLeftNode(where.getExpression());
	                    		newExpression.setRightNode(expression);
	                    		where.setExpression(newExpression);
	                    	}
	                    	where.getParameterMap().put(attribute, parameter);
	                    	
	                    	aliasEntity=FromClause.DEFAULTENTITY;
	                    	break;
	                    default:
	                        break;
	                }
	            }
	        }
		return where;
	}

	@SuppressWarnings("unchecked")
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

	@SuppressWarnings("unchecked")
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

	//the alias part is silly due to not organize right in .g file
	@SuppressWarnings("unchecked")
	private static SelectClause parseResult(CommonTree tree) {
		List<CommonTree> childrenList = tree.getChildren();
		SelectClause select = new SelectClause();
		 if (childrenList != null && childrenList.size() > 0) {
			 	String aliasEntity=FromClause.DEFAULTENTITY;
	            for (CommonTree child : childrenList) {
	                switch (child.getType()) {
	                    case NoSqlLexer.STAR:
	                    	String star = child.getText();
	                    	Attribute attributeStar = new Attribute(aliasEntity,star);
	                    	select.addProjection(attributeStar);
	                    	aliasEntity=FromClause.DEFAULTENTITY;
	                    	break;
	                    case NoSqlLexer.ATTR_NAME:
	                    	String attributeName = child.getText();
	                    	Attribute attribute = new Attribute(aliasEntity,attributeName);
	                    	select.addProjection(attribute);
	                    	aliasEntity=FromClause.DEFAULTENTITY;
	                        break;
	                    case NoSqlLexer.ALIAS:
	                    	aliasEntity=child.getText();
	                    	break;
	                    	
	                    default:
	                        break;
	                }
	            }
	        }
		return select;
		
	}



}
