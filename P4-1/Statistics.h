#ifndef STATISTICS_
#define STATISTICS_
#include "ParseTree.h"
#include <iostream>
#include <fstream>
#include <unordered_map>
#include <string.h>
#include <cmath>
#include <utility>
using namespace std;

typedef unordered_map<string, int> string_to_int_mapper;
typedef unordered_map<string, unordered_map<string, int> > string_to_hashmap_mapper;

class Statistics
{

private:
	// HashMap, lookup for relations and its number of tuples
	string_to_int_mapper *relationDataStore;
	// HashMap, lookup for relations with its attributes and num of distinct tuples
	string_to_hashmap_mapper *attributeDataStore;
	bool isApply;

public:
	Statistics();
	Statistics(Statistics &copyMe); // Performs deep copy
	~Statistics();

	void AddRel(char *relName, int numTuples);
	void AddAtt(char *relName, char *attName, int numDistincts);
	void CopyRel(char *oldName, char *newName);

	void Read(char *fromWhere);
	void Write(char *fromWhere);

	void Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
	double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);
};

#endif
