#include <iostream>
#include <gtest/gtest.h>
#include <fstream>
#include "Defs.h"
#include "Optimizer.h"
#include "QueryTreeNode.h"
#include "ParseTree.h"

using namespace std;

extern "C" {
	struct YY_BUFFER_STATE* yy_scan_string(const char*);
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
	//cout << " \n** IMPORTANT: MAKE SURE THE INFORMATION BELOW IS CORRECT **\n";
	//cout << " catalog location: \t" << catalog_path << endl;
	//cout << " tpch files dir: \t" << tpch_dir << endl;
	//cout << " heap files dir: \t" << dbfile_dir << endl;
	//cout << " \n\n";
}
QueryTree *ins_qt = new QueryTree();
Statistics *s = new Statistics();

//Test case to verify tables parsing
TEST(TestCase1, SubTest1) {
    //readconfigurationFile();
    cout<<"\nCreating table for mytable\n"<<endl;
	char* cnf = "CREATE TABLE mytable (att1 INTEGER, att2 DOUBLE, att3 STRING) AS HEAP;";
	yy_scan_string(cnf);
	yyparse();
    
	
	if(createTable){
		// operation to create table
		int result = ins_qt->createTable(createTable);

        ASSERT_TRUE(result == 1);
			
	}
}

//Test case to verify tables parsing
TEST(TestCase2, SubTest2) {
	readconfigurationFile();
    cout<<"\nInserting into mytable\n"<<endl;
	char* cnf = "INSERT 'myfile.txt' INTO mytable;";
	yy_scan_string(cnf);
	yyparse();

    if(insertFile) {
		// load data from file into the table
	    int result = ins_qt->insertFile(insertFile);
        ASSERT_TRUE(result == 1);
	}

}

TEST(TestCase3, SubTest3) {
	//readconfigurationFile();
    cout<<"\nDropping mytable from the database engine\n"<<endl;
	char* cnf = "DROP TABLE mytable;";
	yy_scan_string(cnf);
	yyparse();
    if(dropTableName) {
		// operation to drop table
		int result = ins_qt->dropTable(dropTableName);
			
		ASSERT_TRUE(result == 1);
	}

}


int main(int argc, char **argv) {
	::testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}