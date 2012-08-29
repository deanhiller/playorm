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
	LEFT_OUTER_JOIN;
	INNER_JOIN;
	VALUE_LIST;
	SELECT_RESULTS;
	INT_VAL;
	DEC_VAL;
	STR_VAL;
	SELECT_CLAUSE;
	FROM_CLAUSE;
	JOIN_CLAUSE;
	PARTITIONS_CLAUSE;
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

@parser::members {
  @Override
  public void reportError(RecognitionException e) {
    throw new ParseQueryException(e); 
  }
}

@lexer::members {
  @Override
  public void reportError(RecognitionException e) {
    throw new ParseQueryException(e); 
  }
}

@rulecatch {
    catch (RecognitionException e)
    {
        throw e;
    }
}

statement: (  selectStatement EOF! );

//SELECT PORTION SPECIFIC STUFF
selectStatement: (partitionClause)? selectClause fromClause (joinClause)? (whereClause)? -> fromClause (joinClause)? (partitionClause)? selectClause (whereClause)?;
selectClause: SELECT resultList -> ^(SELECT_CLAUSE resultList);

resultList
 : STAR          -> ^(SELECT_RESULTS STAR)
 | columnList -> ^(SELECT_RESULTS columnList)
 ;
 
columnList:	simpleColumn (COMMA! simpleColumn)* | aliasedColumn (COMMA! aliasedColumn)*;

//PARTITONS CLAUSE SPECIFIC STUFF (for adhoc queries ONLY!!!!)
partitionClause: PARTITIONS partitionList -> ^(PARTITIONS_CLAUSE partitionList);
partitionList: partition (COMMA! partition)*;
partition: simplePartition | partitionBy;
simplePartition: alias LPAREN parameterOrValue RPAREN -> ^(ALIAS[$alias.text] parameterOrValue);
partitionBy: alias LPAREN strVal COMMA parameterOrValue RPAREN -> ^(ALIAS[$alias.text] parameterOrValue strVal);

//FROM CLAUSE SPECIFIC STUFF
fromClause: FROM tableList -> ^(FROM_CLAUSE tableList);
tableList: table (COMMA! table)*;
table: tableWithNoAlias | tableName AS alias -> ^(TABLE_NAME[$tableName.text] ALIAS[$alias.text]);
tableWithNoAlias: tableName -> TABLE_NAME[$tableName.text]; 

//JOIN CLAUSE SPECIFIC STUFF
joinClause: joinList -> ^(JOIN_CLAUSE joinList);
joinList: singleJoinClause (singleJoinClause)*;
singleJoinClause: leftJoin | join;
leftJoin: LEFT JOIN aliasedColumn AS newAlias -> ^(LEFT_OUTER_JOIN aliasedColumn ALIAS[$newAlias.text]);
join: INNER JOIN aliasedColumn AS newAlias -> ^(INNER_JOIN aliasedColumn ALIAS[$newAlias.text]);

//WHERE CLAUSE SPECIFIC STUFF
whereClause: WHERE^ expression;  //NOTE: This should be (expression | orExpr) BUT antlr doesn't like that so need to re-visit
expression: orExpr; //The ^ makes LPAREN a root while the ! makes RPAREN get dropped from example I saw
orExpr: andExpr (OR^ andExpr)*;
andExpr: primaryExpr (AND^ primaryExpr)*;
primaryExpr: colParamExpr | paramColExpr | inExpr | LPAREN! expression RPAREN!;

//An column now is either a simpleColumn OR a aliasedColumn
//NOTE: We need to fix this later so we create a parameterOrValueOrColumn: value | parameter | column
//so that in a join, we could compare two values
colParamExpr:	column (EQ | NE | GT | LT | GE | LE)^ parameterOrValue;
paramColExpr:	parameterOrValue (EQ | NE | GT | LT | GE | LE)^ column;
inExpr:         column IN^ valueList;

parameterOrValue: value | parameter;

column: simpleColumn | aliasedColumn;
//This collapses the child node and renames the token ATTR_NAME while keeping the text of the token
simpleColumn: ID -> ATTR_NAME[$ID.text];
aliasedColumn: (alias)(DOT)(colName) -> ^(ATTR_NAME[$colName.text] ALIAS[$alias.text]);

tableName: ID;
parameterName: ID;
colName: ID;
alias: ID;
newAlias: ID;

//This collapses the child node and renames the token PARAMETER_NAME while keeping the parameter text
parameter: COLON parameterName -> PARAMETER_NAME[$parameterName.text];

valueList: LPAREN value (COMMA value)* RPAREN -> ^(VALUE_LIST value (value)*);
value: intVal | doubleVal | strVal | NULL;

intVal	 :	(MINUS)?INTEGER 
                  -> {$MINUS.text == null}? INT_VAL[$INTEGER.text]
                  -> INT_VAL[$MINUS.text+$INTEGER.text];
doubleVal:   (MINUS)?DECIMAL
                  -> {$MINUS.text == null}? INT_VAL[$DECIMAL.text]
                  -> INT_VAL[$MINUS.text+$DECIMAL.text];
strVal	:	stringA | stringB;
stringA : STRINGA -> STR_VAL[$STRINGA.text];
stringB : STRINGB -> STR_VAL[$STRINGB.text];
	
PARTITIONS: ('P'|'p')('A'|'a')('R'|'r')('T'|'t')('I'|'i')('T'|'t')('I'|'i')('O'|'o')('N'|'n')('S'|'s');
LEFT    :   ('L'|'l')('E'|'e')('F'|'f')('T'|'t');
INNER   :   ('I'|'i')('N'|'n')('N'|'n')('E'|'e')('R'|'r');
JOIN    :   ('J'|'j')('O'|'o')('I'|'i')('N'|'n');
SELECT	:	('S'|'s')('E'|'e')('L'|'l')('E'|'e')('C'|'c')('T'|'t');
FROM	:	('F'|'f')('R'|'r')('O'|'o')('M'|'m');
WHERE	:	('W'|'w')('H'|'h')('E'|'e')('R'|'r')('E'|'e');
NULL    :   ('N'|'n')('U'|'u')('L'|'l')('L'|'l');

// Lexer Rules
ID	:	('a'..'z'|'A'..'Z'|'_') ('a'..'z'|'A'..'Z'|'0'..'9'|'_'|'-')*;

INTEGER :  ('0'..'9')+;
DECIMAL	:	('.' ('0'..'9')+)  | (('0'..'9')+ '.' '0'..'9'*);

STRINGA	:	'"' (options {greedy=false;}: ESC | .)* '"';
STRINGB	:	'\'' (options {greedy=false;}: ESC | .)* '\'';
MINUS : '-';

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
