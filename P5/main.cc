#include <iostream>
#include <fstream>
#include "Defs.h"
#include "Optimizer.h"
#include "QueryTreeNode.h"
#include "ParseTree.h"
#include <time.h>

using namespace std;

extern "C" {
	int yyparse(void);   // defined in y.tab.c
	int yyfuncparse(void);   // defined in yyfunc.tab.c
}

//Create table
extern struct CreateTable *createTable;
extern struct InsertFile *insertFile;
extern char *dropTableName;
extern char *setOutPut;

extern struct FuncOperator *finalFunction; // the aggregate function (NULL if no agg)
extern struct TableList *tables; // the list of tables and aliases in the query
extern struct AndList *boolean; // the predicate in the WHERE clause
extern struct NameList *groupingAtts; // grouping atts (NULL if no grouping)
extern struct NameList *attsToSelect; // the set of attributes in the SELECT (NULL if no such atts)
extern int distinctAtts; // 1 if there is a DISTINCT in a non-aggregate query
extern int distinctFunc;  // 1 if there is a DISTINCT in an aggregate query


char catalog_path[50];
char dbfile_dir[50];
char tpch_dir[50];
int RUNLEN;

void readconfigurationFile(){

    // test settings file should have the 
    // catalog_path, dbfile_dir and tpch_dir information in separate lines
  const char *settings = "test.cat";

 	FILE *fp = fopen (settings, "r");
	if (fp) {
		char line[80];
		fgets (line, 80, fp);
		sscanf (line, "%s\n", catalog_path);
		fgets (line, 80, fp);
		sscanf (line, "%s\n", dbfile_dir);
		fgets (line, 80, fp);
		sscanf (line, "%s\n", tpch_dir);
		fgets (line, 80, fp);
		RUNLEN = stoi(line);
		fclose (fp);

		if (! (catalog_path && dbfile_dir && tpch_dir)) {
			cerr << " Test settings file 'test.cat' not in correct format.\n";
			exit (1);
		}
	}
	cout << " \n** IMPORTANT: MAKE SURE THE INFORMATION BELOW IS CORRECT **\n";
	cout << " catalog location: \t" << catalog_path << endl;
	cout << " tpch files dir: \t" << tpch_dir << endl;
	cout << " heap files dir: \t" << dbfile_dir << endl;
	cout << " \n\n";
}


int main() {
	// read information from test.cat to access the bin and metadata
	readconfigurationFile();
	cout<<"Database engine started!"<<endl<<endl;
	//int flag = 1;
	
	cout <<"enter query:" << endl;
	yyparse();

	QueryTree *ins_qt = new QueryTree();
	
	if(createTable){
		// operation to create table
		if(ins_qt->createTable(createTable)) {
			cout <<"table created "<<createTable->tableName<<endl;
		}
	}else if(insertFile) {
		// load data from file into the table
		if(ins_qt->insertFile(insertFile)) {
			cout <<"File "<<insertFile->fileName<<" pushed  into " <<insertFile->tableName<<endl;
		}
	} else if(dropTableName) {
		// operation to drop table
		if(ins_qt->dropTable(dropTableName)) {
			cout <<"DBFile is dropped "<<dropTableName<<endl;
		}
	} else if(setOutPut) {
		// operation to setoutput ad required
		ins_qt->output = setOutPut;
		FILE *wfp = fopen(output_path, "w");
		fprintf(wfp, "%s", setOutPut);
		fclose(wfp);
		cout <<"Ouput is set to "<<setOutPut<<endl;
	} else if(tables){ 
		// operation to execute the query
		Statistics *s = new Statistics();
		// load the statistics info 
		s->loadStatistics();
		// contruct the optimizer from the extern data structures 
		Optimizer optimizer(finalFunction, tables, boolean, groupingAtts,
						attsToSelect, distinctAtts, distinctFunc, s);
		// get an optimized query tree for esecution using the statistics
		QueryTree *qt = optimizer.getOptimizedQueryTree();
		if(qt == NULL) {
			cerr <<"error in building query tree!"<<endl;
			exit(0);
		}
		// execute the query tree
		qt->executeQueryTree();
	
	}
	//cout<<"Enter 1 to continue or 0 to stop Database Engine!"<<endl;
	//cin>>flag;
	
	return 0;
}


