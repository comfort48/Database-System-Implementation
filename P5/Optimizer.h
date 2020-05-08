// enumerate all possible query plan to find the optimal one
#ifndef OPTIMIZER_H_
#define OPTIMIZER_H_

#include <map>
//#include <utility>
#include <vector>
#include <string>
#include <string.h>
#include <iostream>
#include "ParseTree.h"
#include "Statistics.h"
#include "QueryTreeNode.h"
#include "Record.h"
//#include "Defs.h"
#include <stdio.h>
#include "Schema.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Function.h"

//char *catalog_path;
//char *dbfile_dir;
//char *tpch_dir;

class Optimizer
{
private:
	struct FuncOperator *finalFunction; //function in aggregation
	struct TableList *tables;			// Tables in FROM CLAUSE
	struct AndList *cnfAndList;			// AndList from WHER CLAUSE
	struct NameList *groupAtts;			// grouping atts (NULL if no grouping)
	struct NameList *selectAtts;		// the set of attributes in the SELECT (NULL if no such atts)
	int distinctAtts;					// 1 if there is a DISTINCT in a non-aggregate query
	int distinctFunc;					// 1 if there is a DISTINCT in an aggregate query

	Statistics *statistics;

	void getPredicateInfo(vector<AndList *> &joins, vector<AndList *> &selects,
						  vector<AndList *> &selAboveJoin);

	map<string, AndList *> *selectionsOptimizeApply(vector<AndList *> selects);
	vector<AndList *> *optimizeJoins(vector<AndList *> joins);

	void getPredicateInfo(vector<AndList> &joinsVector, vector<AndList> &selectsVector, vector<AndList> &joinDepSelectsVector, Statistics statsObj);

	Function *GenerateFunc(Schema *schema);
	OrderMaker *GenerateOM(Schema *schema);

public:
	Optimizer();
	Optimizer(struct FuncOperator *finalFunction,
			  struct TableList *tables,
			  struct AndList *boolean,
			  struct NameList *pGrpAtts,
			  struct NameList *pAttsToSelect,
			  int distinct_atts, int distinct_func, Statistics *);

	QueryTree *getOptimizedQueryTree();

	virtual ~Optimizer();
};

#endif /* OPTIMIZER_H_ */
