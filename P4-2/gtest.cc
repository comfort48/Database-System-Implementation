#include <iostream>
#include <gtest/gtest.h>
#include <map>
#include <vector>
#include <iostream>
#include <assert.h>
#include <string.h>
#include "QueryTreeNode.h"
#include "ParseTree.h"
#include "Statistics.h"

extern "C"
{
	int yyparse(void);
	struct YY_BUFFER_STATE* yy_scan_string(const char*);

}
extern struct FuncOperator* finalFunction;
extern struct TableList* tables;
extern struct AndList* boolean;
extern struct NameList* groupingAtts;
extern struct NameList* attsToSelect;
extern int distinctAtts;
extern int distinctFunc;

void getPredicateInfo(vector<AndList>& joinsVector, vector<AndList>& selectsVector, vector<AndList>& joinDepSelectsVector, Statistics statsObj) {
	
	struct OrList *currentOrList;
	// iterating over the predicate information
	while(boolean != 0) {
		currentOrList = boolean->left;
		// join operation if both left and right is name with equals sign
		if(currentOrList && currentOrList->left->code == EQUALS && currentOrList->left->left->code == NAME && currentOrList->left->right->code == NAME) {
			AndList newAnd = *boolean;
			newAnd.rightAnd = 0;
			joinsVector.push_back(newAnd);
		}
		else {
		// operation on table attribute and literal
			currentOrList = boolean->left;
			if(currentOrList->left == NULL) {
				// predicate on only one table such as (a.b = 'c')
				AndList newAnd = *boolean;
				newAnd.rightAnd = NULL;
				selectsVector.push_back(newAnd);
			}
			else {
				// predicate on multiple tables such as (a.b = 'c' OR x.y = 'z')
				vector<string> tablesInvolvedVector;
				while(currentOrList != NULL ) {
					Operand *oprnd = currentOrList->left->left;
					if(oprnd->code != NAME) {
						oprnd = currentOrList->left->right;
					}
					string relation;
					statsObj.parseRelation(oprnd, relation);
					if(tablesInvolvedVector.size() == 0) {
						tablesInvolvedVector.push_back(relation);
					}
					else if(relation.compare(tablesInvolvedVector[0]) != 0) {
						tablesInvolvedVector.push_back(relation);
					}
					currentOrList = currentOrList->rightOr;
				}
				if(tablesInvolvedVector.size() > 1) {
					// multiple tables involved 
					AndList newAnd = *boolean;
					newAnd.rightAnd = 0;
					joinDepSelectsVector.push_back(newAnd);
				}
				else {
					// single table involved 
					AndList newAnd = *boolean;
					newAnd.rightAnd = 0;
					selectsVector.push_back(newAnd);
				}
			}

		}
		boolean = boolean->rightAnd;
	}
}

vector<AndList> optmizeOrderofJoins(vector<AndList> joinsVector, Statistics *statsObj) {
	// Left deep join Optimization is used 
	vector<AndList> newOrderVec;
	newOrderVec.reserve(joinsVector.size());
	AndList join;
	double smallVal = -1.0;
	double guessVal = 0.0;

	string relation1, relation2;
	int index = 0, smallIndex = 0, counter = 0;
	vector<string> alreadyJoinedRelations;

	// loop over the all join tables ends untill all are processed
	while(joinsVector.size() > 1) {
		while(joinsVector.size() > index) {
			join = joinsVector[index];
			// getting the relation name 
			statsObj->parseRelation(join.left->left->left, relation1);
			statsObj->parseRelation(join.left->left->right, relation2);

			if(smallVal == -1.0) {
				// initial case where the estimate cost is computed for first join
				char *relations[] = {(char*)relation1.c_str(), (char*)relation2.c_str()};
				smallVal = statsObj->Estimate(&join, relations, 2);
				smallIndex = index;
			}
			else {
				// join cost is computed 
				char *relations[] = {(char*)relation1.c_str(), (char*)relation2.c_str()};
				guessVal = statsObj->Estimate(&join, relations, 2);
				// comparing the estimated with the already min cost and updating index accordingly
				if(smallVal > guessVal) {
					smallVal = guessVal;
					smallIndex = index;
				}
			}
			index++;
		}

		alreadyJoinedRelations.push_back(relation1);
		alreadyJoinedRelations.push_back(relation2);
		// join operation of the smallest index is pushed to vector
		newOrderVec.push_back(joinsVector[smallIndex]);

		smallVal = -1.0;
		index = 0;
		counter++;
		joinsVector.erase(joinsVector.begin()+smallIndex);

	}
	// filling last cell with the initial join operation
	newOrderVec.insert(newOrderVec.begin() + counter, joinsVector[0]);
	return newOrderVec;
}

int main(int argc, char **argv) {
	::testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}

//Test case to verify tables parsing
TEST(TestCase1, SubTest1) {
	char* cnf = "SELECT SUM (ps.ps_supplycost), s.s_suppkey FROM part AS p, supplier AS s, partsupp AS ps WHERE (p.p_partkey = ps.ps_partkey) AND (s.s_suppkey = ps.ps_suppkey) AND (s.s_acctbal > 2500.0) GROUP BY s.s_suppkey";
	yy_scan_string(cnf);
	yyparse();

	Statistics* s = new Statistics();

	vector<string> relations;
	vector<AndList> joinsVector;
	vector<AndList> selectsVector;
	vector<AndList> joinDepSelsVector;

	TableList *tList = tables;
	while(tList) {
		if(tList->aliasAs) {
			relations.push_back(tList->aliasAs);
		}
		else {
			relations.push_back(tList->tableName);
		}
		tList = tList->next;
	}

	ASSERT_TRUE(relations.size() == 3);


	getPredicateInfo(joinsVector, selectsVector, joinDepSelsVector, *s);
	ASSERT_TRUE(joinsVector.size() == 2);
	ASSERT_TRUE(selectsVector.size() == 1);
	ASSERT_TRUE(joinDepSelsVector.size() == 0);


	if (joinsVector.size() > 1)
	{
		joinsVector = optmizeOrderofJoins(joinsVector, s);
		ASSERT_TRUE(joinsVector.size() == 2);
	}
}

//Test case to verify tables parsing
TEST(TestCase2, SubTest2) {
	char* cnf = "SELECT n.n_nationkey FROM nation AS n WHERE (n.n_name = 'UNITED STATES')";
	yy_scan_string(cnf);
	yyparse();

	Statistics* s = new Statistics();

	vector<string> relations;
	vector<AndList> joinsVector;
	vector<AndList> selectsVector;
	vector<AndList> joinDepSelsVector;

	TableList *tList = tables;
	while(tList) {
		if(tList->aliasAs) {
			relations.push_back(tList->aliasAs);
		}
		else {
			relations.push_back(tList->tableName);
		}
		tList = tList->next;
	}

	ASSERT_TRUE(relations.size() == 1);


	getPredicateInfo(joinsVector, selectsVector, joinDepSelsVector, *s);
	ASSERT_TRUE(joinsVector.size() == 0);
	ASSERT_TRUE(selectsVector.size() == 1);
	ASSERT_TRUE(joinDepSelsVector.size() == 0);


	if (joinsVector.size() > 1)
	{
		joinsVector = optmizeOrderofJoins(joinsVector, s);
		ASSERT_TRUE(joinsVector.size() == 0);
	}
}

//Test case to verify tables parsing
TEST(TestCase3, SubTest3) {
	char* cnf = "SELECT SUM (n.n_nationkey) FROM nation AS n, region AS r WHERE (n.n_regionkey = r.r_regionkey) AND (n.n_name = 'UNITED STATES')";
	yy_scan_string(cnf);
	yyparse();

	Statistics* s = new Statistics();

	vector<string> relations;
	vector<AndList> joinsVector;
	vector<AndList> selectsVector;
	vector<AndList> joinDepSelsVector;

	TableList *tList = tables;
	while(tList) {
		if(tList->aliasAs) {
			relations.push_back(tList->aliasAs);
		}
		else {
			relations.push_back(tList->tableName);
		}
		tList = tList->next;
	}

	ASSERT_TRUE(relations.size() == 2);


	getPredicateInfo(joinsVector, selectsVector, joinDepSelsVector, *s);
	ASSERT_TRUE(joinsVector.size() == 1);
	ASSERT_TRUE(selectsVector.size() == 1);
	ASSERT_TRUE(joinDepSelsVector.size() == 0);


	if (joinsVector.size() > 1)
	{
		joinsVector = optmizeOrderofJoins(joinsVector, s);
		ASSERT_TRUE(joinsVector.size() == 1);
	}
}


//Test case to verify tables parsing
TEST(TestCase4, SubTest4) {
	char* cnf = "SELECT SUM (ps.ps_supplycost), s.s_suppkey FROM part AS p, supplier AS s, partsupp AS ps WHERE (p.p_partkey = ps.ps_partkey) AND (s.s_suppkey = ps.ps_suppkey) AND (s.s_acctbal > 2500.0 OR p.p_partkey < 100)";
	yy_scan_string(cnf);
	yyparse();

	Statistics* s = new Statistics();

	vector<string> relations;
	vector<AndList> joinsVector;
	vector<AndList> selectsVector;
	vector<AndList> joinDepSelsVector;

	TableList *tList = tables;
	while(tList) {
		if(tList->aliasAs) {
			relations.push_back(tList->aliasAs);
		}
		else {
			relations.push_back(tList->tableName);
		}
		tList = tList->next;
	}

	ASSERT_TRUE(relations.size() == 3);


	getPredicateInfo(joinsVector, selectsVector, joinDepSelsVector, *s);
	ASSERT_TRUE(joinsVector.size() == 2);
	ASSERT_TRUE(selectsVector.size() == 0);
	ASSERT_TRUE(joinDepSelsVector.size() == 1);

	if (joinsVector.size() > 1)
	{
		joinsVector = optmizeOrderofJoins(joinsVector, s);
		ASSERT_TRUE(joinsVector.size() == 2);
	}
}