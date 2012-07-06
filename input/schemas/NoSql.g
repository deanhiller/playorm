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
	IN_PARENS;

	AS	=	'as';
	DOT	=	'.';
	EQ	=	'=';
	NE	=	'!=';
	GT	=	'>';
	GE	=	'>=';
	LT	=	'<';
	LE	=	'<=';
	LPAREN 	=	'(';
	RPAREN   =	')';
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

statement: (  selectStatement  );

//SELECT PORTION SPECIFIC STUFF
selectStatement: selectClause fromClause (whereClause)?;
selectClause: SELECT resultList -> ^(SELECT_CLAUSE resultList);
resultList:	(STAR | attributeList |) -> ^(RESULT attributeList? STAR?);	
attributeList:	simpleAttribute (COMMA! simpleAttribute)* | aliasdAttribute (COMMA! aliasdAttribute)*;

//FROM CLAUSE SPECIFIC STUFF
fromClause: FROM tableList -> ^(FROM_CLAUSE tableList);
tableList: table (COMMA! table)*;
table: tableWithNoAlias | tableName alias -> ^(TABLE_NAME[$tableName.text] ALIAS[$alias.text]);
tableWithNoAlias: tableName -> TABLE_NAME[$tableName.text]; 

//WHERE CLAUSE SPECIFIC STUFF
whereClause: WHERE^ orExpr;  //NOTE: This should be (expression | orExpr) BUT antlr doesn't like that so need to re-visit
expression: LPAREN^ orExpr RPAREN!;
orExpr: andExpr (OR^ andExpr)*;
andExpr: primaryExpr (AND^ primaryExpr)*;
primaryExpr: parameterExpr | inExpr | compExpr | expression;

//An attribute now is either a simpleAttribute OR a aliasdAttribute
parameterExpr:	attribute (EQ | NE | GT | LT | GE | LE)^ parameter;
inExpr:         attribute IN^ valueList;
compExpr:       attribute (EQ | NE | GT | LT | GE | LE)^ value;

attribute: simpleAttribute | aliasdAttribute;
//This collapses the child node and renames the token ATTR_NAME while keeping the text of the token
simpleAttribute: ID -> ATTR_NAME[$ID.text];
aliasdAttribute: (alias)(DOT)(attrName) -> ^(ATTR_NAME[$attrName.text] ALIAS[$alias.text]);

tableName: ID;
parameterName: ID;
attrName: ID;
alias: ID;

//This collapses the child node and renames the token PARAMETER_NAME while keeping the parameter text
parameter: COLON parameterName -> PARAMETER_NAME[$parameterName.text];

valueList: LPAREN value (COMMA value)* RPAREN -> ^(VALUE_LIST value (value)*);
value: intVal | doubleVal | strVal;

intVal	:	INTEGER -> INT_VAL[$INTEGER.text];
doubleVal   :   DECIMAL -> DEC_VAL[$DECIMAL.text];
strVal	:	stringA | stringB;
stringA : STRINGA -> STR_VAL[$STRINGA.text];
stringB : STRINGB -> STR_VAL[$STRINGB.text];
	
SELECT	:	('S'|'s')('E'|'e')('L'|'l')('E'|'e')('C'|'c')('T'|'t');
FROM	:	('F'|'f')('R'|'r')('O'|'o')('M'|'m');
WHERE	:	('W'|'w')('H'|'h')('E'|'e')('R'|'r')('E'|'e');
// Lexer Rules
ID	:	('a'..'z'|'A'..'Z'|'_') ('a'..'z'|'A'..'Z'|'0'..'9'|'_')*;

INTEGER :  ('0'..'9')+;
DECIMAL	:	('.' ('0'..'9')+)  | (('0'..'9')+ '.' '0'..'9'*);

STRINGA	:	'"' (options {greedy=false;}: ESC | .)* '"';
STRINGB	:	'\'' (options {greedy=false;}: ESC | .)* '\'';

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
