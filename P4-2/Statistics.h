#ifndef STATISTICS_
#define STATISTICS_
#include "ParseTree.h"
#include "RelationStatistics.h"
#include <iostream>
#include <fstream>
#include <unordered_map>
#include <string.h>
#include <cmath>
#include <utility>

using namespace std;

class Statistics
{


public:

	vector<RelationStatSet> listOfSets;
	map<string, RelationStatistics> listOfRelations;
	map<string, RelationStatSet> relationSets;
	map<string, string> tableNames;

	Statistics();
	Statistics(Statistics &copyMe); // Performs deep copy
	~Statistics();

	Statistics& operator= (Statistics& assignMe);

	void AddRel(char *relName, int numTuples);
	void AddRel(char *relName, double numTuples);
	void AddAtt(char *relName, char *attName, int numDistincts);
	void CopyRel(char *oldName, char *newName);

	void Read(char *fromWhere);
	void Write(char *fromWhere);

	void Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
	double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);

	double guessEstimate(struct AndList *parseTree, RelationStatSet toEstimate, vector<int> &indexVec, double joinEstimate);
	double parseJoin(struct AndList *parseTree);
	double getCountOfTuples(string rel, double joinEstimate);

	void parseRelationAndAttribute(struct Operand *oper, string &rel, string &attribute);
	void parseRelation(struct Operand *oper, string &rel);
	
};

#endif
