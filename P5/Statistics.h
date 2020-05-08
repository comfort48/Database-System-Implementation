#ifndef STATISTICS_
#define STATISTICS_
#include "ParseTree.h"
#include <unordered_map>
#include <string>
#include <cstring>
#include <cstdlib>
#include <vector>
#include <set>
#include <iostream>
#include <stdio.h>

using namespace std;

struct Rel_Stat
{
	unsigned long int numTuples; 
	unsigned long int numTuplesForWritten;
	int partition; // the partition this relation belongs to
	bool copied;   // identify whether this relation is copied or not
	unordered_map<string, unsigned long int> atts;

	Rel_Stat() : numTuples(0), numTuplesForWritten(0), partition(-1), copied(false)
	{
	}

	Rel_Stat(Rel_Stat *copyMe)
	{
		numTuples = copyMe->numTuples;
		partition = copyMe->partition;
		numTuplesForWritten = copyMe->numTuplesForWritten;
		copied = true;
		unordered_map<string, unsigned long int>::iterator it;
		for (it = copyMe->atts.begin(); it != copyMe->atts.end(); it++)
		{
			atts.insert(make_pair(it->first, it->second));
		}
	}
};

class Statistics
{
private:
	struct Partition
	{
		vector<string> *relations;
		unsigned long int numTuples;

		Partition() : numTuples(0)
		{
			relations = new vector<string>;
		}
		Partition(Partition *copyMe)
		{
			relations = new vector<string>;
			numTuples = copyMe->numTuples;
			for (vector<string>::iterator it = copyMe->relations->begin(); it != copyMe->relations->end(); it++)
			{
				relations->push_back(*it);
			}
		}
		~Partition()
		{
			delete relations;
		}
	};

	int m_partitionNum;

	unordered_map<string, Rel_Stat *> m_rel_stat;
	unordered_map<string, vector<string> *> m_att_to_rel;
	unordered_map<int, Partition *> m_partition;

	//private variables and methods for estimate and apply
	vector<string> join_rels;
	//	bool ErrorChecking(struct AndList *parseTree, char *relNames[], int numToJoin);
	unsigned long int findNumDistinctsWithName(string, char *relNames[], int, bool joining);

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

	unordered_map<string, Rel_Stat *> *GetRelStat()
	{
		return &m_rel_stat;
	}
	unordered_map<int, Partition *> *GetPartition()
	{
		return &m_partition;
	}
	unordered_map<string, vector<string> *> *GetAttToRel()
	{
		return &m_att_to_rel;
	}

	int ParseRelation(string, string &);
	void loadStatistics();
};

#endif
