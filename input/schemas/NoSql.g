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
	BOOL_VAL;
	SELECT_CLAUSE;
	FROM_CLAUSE;
	JOIN_CLAUSE;
	PARTITIONS_CLAUSE;
	WHERE_CLAUSE;
	ALIAS;
	IN_PARENS;
	UPDATE_CLAUSE;
	TABLE_CLAUSE;
	DELETE_COLUMN;

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

statement: ( sqlStatement EOF! );
sqlStatement: selectStatement | updateStatement | deleteStatement | deleteColumnStatement;

//SELECT PORTION SPECIFIC STUFF
selectStatement: (partitionClause)? selectClause fromClause (joinClause)? (whereClause)? -> fromClause (joinClause)? (partitionClause)? selectClause (whereClause)?;
selectClause: SELECT resultList -> ^(SELECT_CLAUSE resultList);

resultList
 : STAR          -> ^(SELECT_RESULTS STAR)
 | columnList -> ^(SELECT_RESULTS columnList)
 ;

columnList:	simpleColumn (COMMA! simpleColumn)* | aliasedColumn (COMMA! aliasedColumn)*;

// UPDATE statement
updateStatement: UPDATE tableClause (joinClause)? updateClause (whereClause)+ -> tableClause (joinClause)? (whereClause)+ updateClause UPDATE;
tableClause: table -> ^(TABLE_CLAUSE table);
updateClause: SET LPAREN updateCondition (COMMA updateCondition)* RPAREN -> ^(UPDATE_CLAUSE updateCondition (updateCondition)*);
updateCondition:  column EQ^ value;

//DELETE statement
deleteStatement: deleteClause fromClause (whereClause)? -> fromClause (whereClause)? deleteClause;
deleteClause: DELETE deleteList;
deleteList: (STAR)?;

//DELETECOLUMN statement
deleteColumnStatement: deleteColumnClause fromClause (whereClause)? -> fromClause (whereClause)? deleteColumnClause;
deleteColumnClause : DELETECOLUMN deleteColumn;
deleteColumn: LPAREN column (COMMA value)? RPAREN -> ^(DELETE_COLUMN column (value)?);

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
primaryExpr: colParamExpr | paramColExpr | between | inExpr | LPAREN! expression RPAREN!;

//An column now is either a simpleColumn OR a aliasedColumn
//NOTE: We need to fix this later so we create a parameterOrValueOrColumn: value | parameter | column
//so that in a join, we could compare two values
colParamExpr:	column operator^ parameterOrValue;
paramColExpr:	parameterOrValue operator column -> ^(operator column parameterOrValue);
//colToCol:       column operator^ column;
inExpr:         column IN^ valueList;
between:        column BETWEEN a=parameterOrValue AND b=parameterOrValue -> ^(BETWEEN column $a $b);

operator: EQ | NE | GT | LT | GE | LE;

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
value: intVal | doubleVal | strVal | booleanVal | NULL;

booleanVal: booleanAtom -> BOOL_VAL[$booleanAtom.text];
booleanAtom: TRUE | FALSE;

intVal	 :	(MINUS)?INTEGER 
                  -> {$MINUS.text == null}? INT_VAL[$INTEGER.text]
                  -> INT_VAL[$MINUS.text+$INTEGER.text];
doubleVal:   (MINUS)?DECIMAL
                  -> {$MINUS.text == null}? DEC_VAL[$DECIMAL.text]
                  -> DEC_VAL[$MINUS.text+$DECIMAL.text];
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
BETWEEN :   ('B'|'b')('E'|'e')('T'|'t')('W'|'w')('E'|'e')('E'|'e')('N'|'n');
FALSE   :   ('F'|'f')('A'|'a')('L'|'l')('S'|'s')('E'|'e');
TRUE    :   ('T'|'t')('R'|'r')('U'|'u')('E'|'e');
UPDATE  :   ('U'|'u')('P'|'p')('D'|'d')('A'|'a')('T'|'t')('E'|'e');
DELETE  :   ('D'|'d')('E'|'e')('L'|'l')('E'|'e')('T'|'t')('E'|'e');
SET		:	('S'|'s')('E'|'e')('T'|'t');
DELETECOLUMN : 	('D'|'d')('E'|'e')('L'|'l')('E'|'e')('T'|'t')('E'|'e')('C'|'c')('O'|'o')('L'|'l')('U'|'u')('M'|'m')('N'|'n');
AND		:	('A'|'a')('N'|'n')('D'|'d');
OR		: 	('O'|'o')('R'|'r');
NOT		:	('N'|'n')('O'|'o')('T'|'t');
IN		:	('I'|'i')('N'|'n');
AS		:	('A'|'a')('S'|'s');

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
