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
	SELECT	=	'select';
	FROM	=	'from';
	WHERE	=	'where';
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
	:	(STAR | attributeList) -> ^(RESULT attributeList? STAR?)
	;
	
attributeList
	:	attribute (COMMA! attribute)*
	;
table
	: 	tableName -> ^(TABLE_NAME tableName)
	;

attribute	
	:	attrName -> ^(ATTR_NAME attrName)
	;
	
attrName
	:	ID
	;

tableName
	:	ID
	;

parameterName
	:	ID
	;
expression
	:	orExpr
	;

orExpr	:	andExpr (OR^ andExpr)*
	;

andExpr	:	notExpr (AND^ notExpr)*
	;
	
notExpr
	:	(NOT^)? primaryExpr
	;

primaryExpr
	:	compExpr
	|	inExpr
	|	parameterExpr
	|	attribute
	;
parameterExpr
	:	attribute (EQ | NE | GT | LT | GE | LE)^ parameter
	;
	
compExpr
	:	attribute (EQ | NE | GT | LT | GE | LE)^ value
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
