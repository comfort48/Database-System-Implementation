
#include <iostream>
#include "ParseTree.h"
#include "QueryTreeNode.h"
#include "Statistics.h"

using namespace std;

extern "C" {
	int yyparse(void);   // defined in y.tab.c
}

extern struct NameList *groupingAtts;		// group by attributes in the predicate
extern struct AndList *boolean;				// the Predicate info from Where clause in SQL query
extern struct TableList *tables; 			// table information form the input SQL query
extern struct FuncOperator *finalFunction;	// information of the aggregate operations in the SQL query
extern struct NameList *attsToSelect;		// attributes in the select clause 
extern int distinctAtts;					// Distinct present in the non aggregate SQL query
extern int distinctFunc;					// Distinct present in the aggregate SQL query

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

int main () {

	Statistics *statsFileObj = new Statistics();
	// reading the statistics file
	statsFileObj->Read("Statistics.txt");

	int pipeID = 1;

	cout << "enter: ";

	vector<AndList> joinsVector, selectsVector, joinDepSelectsVector;
	map<string, double> joinCostsVector;
	

	vector<string> relations;
	string projectStartString;

	yyparse();
	// reading the table information from the input query	
	TableList *tList = tables;
	while(tList) {
		if(tList->aliasAs) {
			// adding alias name of table to vector if exists
			relations.push_back(tList->aliasAs);
		}
		else {
			// else adding the table name to vector
			relations.push_back(tList->tableName);
		}
		tList = tList->next;
	}
	
	getPredicateInfo(joinsVector, selectsVector, joinDepSelectsVector, *statsFileObj);

	cout << endl;
	cout << "Number of selects: " << selectsVector.size() << endl;
	cout << "Number of joins: " << joinsVector.size() << endl;

	TableList *tableIterater = tables;
	// Map need to hold each table and its QueryTree for Select Operation
	map<string, QueryTreeNode*> leaves;
	QueryTreeNode *nodeForInsertion = NULL;
	QueryTreeNode *traversalNode;
	QueryTreeNode *topLevelNode = NULL;

	// iterate over the tables in the query
	while(tableIterater != 0) {

		if(tableIterater->aliasAs != 0) {
			// if table name have alias add to Map and create table info for alias 
			leaves.insert(make_pair(tableIterater->aliasAs, new QueryTreeNode()));
			statsFileObj->CopyRel(tableIterater->tableName, tableIterater->aliasAs);
		}
		else {
			// else case insert into Map 
			leaves.insert(pair<string, QueryTreeNode*>(tableIterater->tableName, new QueryTreeNode()));
		}

		nodeForInsertion = leaves[tableIterater->aliasAs];
		nodeForInsertion->schema = new Schema("catalog", tableIterater->tableName);
		if(tableIterater->aliasAs != 0) {
			//updating the alias name for table in the schema
			nodeForInsertion->schema->updateName(string(tableIterater->aliasAs));
		}
		topLevelNode = nodeForInsertion;
		nodeForInsertion->outputPipeID = pipeID++;
		string baseStr(tableIterater->tableName);
		string path("bin/"+baseStr+".bin");

		nodeForInsertion->filePath = strdup(path.c_str());
		// declaring the select operation for each table in the query tree Node
		nodeForInsertion->setQueryTreeNodeType(SELECTFILE);

		tableIterater = tableIterater->next;
	}

	AndList iteratorForSelect;
	string tableName, attribute;

	// planning the select operations
	for(int i = 0; i < selectsVector.size(); i++) {
		iteratorForSelect = selectsVector[i];
		if(iteratorForSelect.left->left->left->code == NAME) {
			statsFileObj->parseRelation(iteratorForSelect.left->left->left, tableName);
		}
		else{
			statsFileObj->parseRelation(iteratorForSelect.left->left->right, tableName);
		}

		traversalNode = leaves[tableName];
		projectStartString = tableName;
		while(traversalNode->parentNode != NULL) {
			traversalNode = traversalNode->parentNode;
		}
		// Building the Query Tree Node for the Select Pipe
		nodeForInsertion = new QueryTreeNode();
		traversalNode->parentNode = nodeForInsertion;
		nodeForInsertion->leftNode = traversalNode;
		nodeForInsertion->schema = traversalNode->schema;
		nodeForInsertion->nodeType = SELECTPIPE;
		nodeForInsertion->cnf = &selectsVector[i];
		nodeForInsertion->leftChildPipeID = traversalNode->outputPipeID;
		nodeForInsertion->outputPipeID = pipeID++;

		char *statsApply = strdup(tableName.c_str());
		// applying the statistics for the select operations
		statsFileObj->Apply(&iteratorForSelect, &statsApply, 1);

		topLevelNode = nodeForInsertion;
	}

	if(joinsVector.size() > 1) {
		// if joins exist in the query we need to optimize order of Join for faster execution
		joinsVector = optmizeOrderofJoins(joinsVector, statsFileObj);
	}

	QueryTreeNode *leftTreeNode;
	QueryTreeNode *rightTreeNode;
	AndList currentJoin;
	string relation1, relation2;

	// planning the Join Operations 
	for(int i = 0; i < joinsVector.size(); i++) {
		currentJoin = joinsVector[i];
		// get the relation names involved in the join
		relation1 = "";
		statsFileObj->parseRelation(currentJoin.left->left->left, relation1);
		relation2 = "";
		statsFileObj->parseRelation(currentJoin.left->left->right, relation2);
		tableName = relation1;

		leftTreeNode = leaves[relation1];
		rightTreeNode = leaves[relation2];
		while(leftTreeNode->parentNode != NULL)
			leftTreeNode = leftTreeNode->parentNode;
		while(rightTreeNode->parentNode != NULL)
			rightTreeNode = rightTreeNode->parentNode;
		
		// building the Query Tree Node for the Join Operation
		nodeForInsertion = new QueryTreeNode();
		nodeForInsertion->nodeType = JOIN;
		nodeForInsertion->leftChildPipeID = leftTreeNode->outputPipeID;
		nodeForInsertion->rightChildPipeID = rightTreeNode->outputPipeID;
		nodeForInsertion->outputPipeID = pipeID++;
		nodeForInsertion->cnf = &joinsVector[i];

		nodeForInsertion->leftNode = leftTreeNode;
		nodeForInsertion->rightNode = rightTreeNode;

		leftTreeNode->parentNode = nodeForInsertion;
		rightTreeNode->parentNode = nodeForInsertion;

		nodeForInsertion->generateSchema();
		topLevelNode = nodeForInsertion;

	}

	// planning the selects on multiple tables which are dependent on joins in where clause
	for(int i = 0; i < joinDepSelectsVector.size(); i++) {
		traversalNode = topLevelNode;
		// building the query tree node 
		nodeForInsertion = new QueryTreeNode();
		traversalNode->parentNode = nodeForInsertion;
		nodeForInsertion->leftNode = traversalNode;
		nodeForInsertion->schema = traversalNode->schema;
		nodeForInsertion->nodeType = SELECTPIPE;
		nodeForInsertion->cnf = &joinDepSelectsVector[i];
		nodeForInsertion->leftChildPipeID = traversalNode->outputPipeID;
		nodeForInsertion->outputPipeID = pipeID++;
		topLevelNode = nodeForInsertion;
	}

	// planning for aggregagte function operations
	if(finalFunction != NULL) {
		// distinct operation over aggregated operations 
		if(distinctFunc == 1) {
			nodeForInsertion = new QueryTreeNode();
			// building query tree node 
			nodeForInsertion->nodeType = DISTINCT;
			nodeForInsertion->leftNode = topLevelNode;
			nodeForInsertion->leftChildPipeID = topLevelNode->outputPipeID;
			nodeForInsertion->outputPipeID = pipeID++;
			nodeForInsertion->schema = topLevelNode->schema;
			topLevelNode->parentNode = nodeForInsertion;
			topLevelNode = nodeForInsertion;
			// nodeForInsertion->functionOperator = finalFunction;
			// nodeForInsertion->generateFunction();
		}
		// Normal SUM operations in the predicate
		if(groupingAtts == NULL) {
			nodeForInsertion = new QueryTreeNode();
			// building the query tree node 
			nodeForInsertion->nodeType = SUM;
			nodeForInsertion->leftNode = topLevelNode;
			topLevelNode->parentNode = nodeForInsertion;

			nodeForInsertion->leftChildPipeID = topLevelNode->outputPipeID;

			nodeForInsertion->outputPipeID = pipeID++;
			nodeForInsertion->functionOperator = finalFunction;
			nodeForInsertion->schema = topLevelNode->schema;
			nodeForInsertion->generateFunction();

		}
		else {
			//group by operation present in the predicate
			nodeForInsertion = new QueryTreeNode();
			// building the query tree node 
			nodeForInsertion->nodeType = GROUPBY;
			nodeForInsertion->leftNode = topLevelNode;
			nodeForInsertion->parentNode = nodeForInsertion;
			nodeForInsertion->leftChildPipeID = topLevelNode->outputPipeID;
			nodeForInsertion->outputPipeID = pipeID++;
			nodeForInsertion->schema = topLevelNode->schema;

			nodeForInsertion->oMaker = new OrderMaker();

			int numberOfAttributesGroups = 0;
			vector<int> attributesToGroup;
			vector<int> whichType;

			NameList* grpTrav = groupingAtts;
			// getting the arttributes for the GroupBy
			while(grpTrav) {
				numberOfAttributesGroups++;
				attributesToGroup.push_back(nodeForInsertion->schema->Find(grpTrav->name));
				whichType.push_back(nodeForInsertion->schema->FindType(grpTrav->name));
				cout<<"GROUPING ON "<<grpTrav->name<<endl;
				grpTrav = grpTrav->next;

			}
			// creating the ordermaker groupby operation
			nodeForInsertion->generateOrderMaker(numberOfAttributesGroups, attributesToGroup, whichType);
			nodeForInsertion->functionOperator = finalFunction;
			nodeForInsertion->generateFunction();
		}
		topLevelNode = nodeForInsertion;
	}

	// planning the distinct attributes over non aggregated query
	if(distinctAtts == 1) {
		nodeForInsertion = new QueryTreeNode();
		// building the query tree node
		nodeForInsertion->nodeType = DISTINCT;
		nodeForInsertion->leftNode = topLevelNode;
		nodeForInsertion->parentNode = nodeForInsertion;
		nodeForInsertion->leftChildPipeID = topLevelNode->outputPipeID;
		nodeForInsertion->outputPipeID = pipeID++;
		nodeForInsertion->schema = topLevelNode->schema;
		topLevelNode = nodeForInsertion;
	}

	// planning the attributes in query for project operation
	if(attsToSelect != NULL) {
		traversalNode = topLevelNode;
		nodeForInsertion = new QueryTreeNode();
		// building the query tree node 
		nodeForInsertion->nodeType = PROJECT;

		nodeForInsertion->leftNode = traversalNode;
		traversalNode->parentNode = nodeForInsertion;
		nodeForInsertion->leftChildPipeID = traversalNode->outputPipeID;
		nodeForInsertion->outputPipeID = pipeID++;
		
		vector<int> indexOfAttributesToKeep;

		Schema *oldSchema = traversalNode->schema;
		NameList *attributesTrav = attsToSelect;
		string attribute;
		// filtering the attributes for the project operation 
		while(attributesTrav != 0) {
			attribute = attributesTrav->name;
			indexOfAttributesToKeep.push_back(oldSchema->Find(const_cast<char*>(attribute.c_str())));
			attributesTrav = attributesTrav->next;
		}
		Schema *newSchema = new Schema(oldSchema, indexOfAttributesToKeep);
		nodeForInsertion->schema = newSchema;
		nodeForInsertion->schema->Print();

	}
	cout<<"PRINTING TREE IN ORDER: "<<endl<<endl;
	if(nodeForInsertion != NULL) {
		// function to print the query plan
		nodeForInsertion->printInOrder();
	}
}