/**
* Grammer file for NoSql Query Language
*
*    @author HuaiJiang
*
**/


grammar NoSql;


options { 
	output = AST; 
	language = Java;
}

tokens {
	ATTR_NAME;
	PARAMETER_NAME;
	TABLE_NAME;
	VALUE_LIST;
	RESULT;
	INT_VAL;
	DEC_VAL;
	STR_VAL;
	SELECT_CLAUSE;
	FROM_CLAUSE;
	WHERE_CLAUSE;
	ALIAS;

	AS	=	'as';
	DOT	=	'.';
	EQ	=	'=';
	NE	=	'!=';
	GT	=	'>';
	GE	=	'>=';
	LT	=	'<';
	LE	=	'<=';
	OPEN 	=	'(';
	CLOSE   =	')';
	STAR	=	'*';
	COMMA	=	',';
	COLON	=	':';
	AND	=	'and';
	OR	=	'or';
	NOT	=	'not';
	IN	=	'in';
	
}

@header {
package com.alvazan.orm.parser.antlr;
}

@lexer::header {
package com.alvazan.orm.parser.antlr;
}


@rulecatch {
    catch (RecognitionException e)
    {
        throw e;
    }
}

statement
	: (  selectStatement  )
	;


selectStatement
	: selectClause
		(fromClause)?
		(whereClause)?
		;
		



selectClause
	: SELECT resultList -> ^(SELECT_CLAUSE resultList) 
	;


	


fromClause
	: FROM tableList -> ^(FROM_CLAUSE tableList) 
	;

whereClause
	: WHERE expression -> ^(WHERE_CLAUSE expression)
	;




	
tableList
	:  	table (COMMA! table)*
	;
	
resultList
	:	(STAR | attributeList |) -> ^(RESULT attributeList? STAR?)
	;
	
attributeList
	:	attribute (COMMA! attribute)*
	|	aliasdAttribute (COMMA! aliasdAttribute)*
	;
table
	: 	tableName -> ^(TABLE_NAME tableName)
	;

attribute	
	:	attrName -> ^(ATTR_NAME attrName)
	;
	
aliasdAttribute
	:       alias DOT attrName -> ^(ALIAS alias ) ^(ATTR_NAME attrName)
	;
	
attrName
	:	ID
	;

tableName
	:	ID
	|	ID alias -> ^(ALIAS alias)
	;

parameterName
	:	ID
	;
expression
	:	orExpr
	;

orExpr	:	andExpr (OR^ andExpr)*
	;

andExpr	:	primaryExpr (AND^ primaryExpr)*
	;
	

primaryExpr
	:	compExpr
	|	inExpr
	|	parameterExpr
	|	attribute
	;
parameterExpr
	:	attribute (EQ | NE | GT | LT | GE | LE)^ parameter
	|	aliasdAttribute (EQ | NE | GT | LT | GE | LE)^parameter
	;
	
compExpr
	:	attribute (EQ | NE | GT | LT | GE | LE)^ value
	|     aliasdAttribute(EQ | NE | GT | LT | GE | LE)^value
	;
alias
	: 	ID
	;
inExpr	:	attribute IN^ valueList
	;

parameter
	: 	COLON parameterName -> ^(PARAMETER_NAME parameterName)
	;

valueList
	:	 value (COMMA value)*  -> ^(VALUE_LIST value (value)*)
	;

value	
    	:	intVal 
    	|   	doubleVal
	|	strVal
	;
	
intVal	:	INTEGER -> ^(INT_VAL INTEGER)
	;

doubleVal   :   DECIMAL -> ^(DEC_VAL DECIMAL)
    ;
    
strVal	:	STRING -> ^(STR_VAL STRING)
	;
	
	
SELECT	:	('S'|'s')('E'|'e')('L'|'l')('E'|'e')('C'|'c')('T'|'t');
FROM	:	('F'|'f')('R'|'r')('O'|'o')('M'|'m');
WHERE	:	('W'|'w')('H'|'h')('E'|'e')('R'|'r')('E'|'e');
// Lexer Rules
ID	:	('a'..'z'|'A'..'Z'|'_') ('a'..'z'|'A'..'Z'|'0'..'9'|'_')*
	;

INTEGER :  ('0'..'9')+
    ;
    
DECIMAL	:	('.' ('0'..'9')+)  | (('0'..'9')+ '.' '0'..'9'*)
	;

STRING	:	'"' (options {greedy=false;}: ESC | .)* '"'
	;

WS	:
 	(   ' '
        |   '\t'
        |   '\r'
        |   '\n'
        )+
        { $channel=HIDDEN; }
        ;  
    
ESC	:
	'\\' ('"'|'\''|'\\')
	;
