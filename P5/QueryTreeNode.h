//Class for to store one query plan for enumeration
#ifndef QUERYTREENODE_H_
#define QUERYTREENODE_H_

#include "RelOp.h"
#include "Pipe.h"
#include <map>
//#include "Defs.h"
#include <string.h>
#include "ParseTree.h"
#include "DBFile.h"
#include <iostream>
#include <fstream>
#include <vector>

using namespace std;

class QueryTreeNode
{
public:

	QueryNodeType opType;

	QueryTreeNode *parent; //useful for join
	QueryTreeNode *left;
	QueryTreeNode *right;

	int lPipeId;
	//	Pipe *lPipe;
	int rPipeId;
	//	Pipe *rPipe;
	int outPipeId;
	//	Pipe *outPipe;

	FILE *outFile; //stdout or file for writeout

	string dbfilePath;

	CNF *cnf;
	Record *literal;
	Schema *outputSchema;
	Function *function;

	OrderMaker *orderMaker;
	//for project
	int *keepMe;
	int numAttsInput, numAttsOutput;

	QueryTreeNode();
	~QueryTreeNode();
};

class QueryTree
{
public:
	QueryTree();
	virtual ~QueryTree();

	QueryTreeNode *root;

	int pipeNum;
	map<int, Pipe *> pipes; //used when execute
	vector<RelationalOp *> operators;

	int dbNum;
	DBFile *dbs[10];

	char *output;

	void PrintNode(QueryTreeNode *);
	void PrintInOrder();
	void ExecuteNode(QueryTreeNode *);
	int executeQueryTree();
	int createTable(CreateTable *);
	int insertFile(InsertFile *);
	int dropTable(char *);
};

#endif
