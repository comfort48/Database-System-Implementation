#include <gtest/gtest.h>
#include <iostream>
#include <stdlib.h>
#include "Statistics.h"
#include "ParseTree.h"
#include <math.h>
extern "C" struct YY_BUFFER_STATE *yy_scan_string(const char *);
extern "C" int yyparse(void);
extern struct AndList *final;

char *statisticsFile = "Statistics.txt";

TEST(GtestForStatisticalEstimation_1, SubTest1)
{

	bool estimationFlag = true;

	Statistics s;
	char *relName[] = {"supplier", "partsupp"};

	s.AddRel(relName[0], 10000);
	s.AddAtt(relName[0], "s_suppkey", 10000);

	s.AddRel(relName[1], 800000);
	s.AddAtt(relName[1], "ps_suppkey", 10000);

	char *cnf = "(s_suppkey = ps_suppkey)";

	yy_scan_string(cnf);
	yyparse();
	double result = s.Estimate(final, relName, 2);
	if ((result - 800000) > 0.1)
		estimationFlag = false;
	s.Apply(final, relName, 2);

	// test write and read
	s.Write(statisticsFile);

	//reload the statistics object from file
	Statistics s1;
	s1.Read(statisticsFile);
	cnf = "(s_suppkey>1000)";
	yy_scan_string(cnf);
	yyparse();
	double dummy = s1.Estimate(final, relName, 2);

	if (fabs(dummy * 3.0 - result) > 0.1)
	{
		estimationFlag = false;
	}

	ASSERT_TRUE(estimationFlag);
}

TEST(GtestForStatisticalEstimation_2, SubTest2)
{

	bool estimationFlag = true;

	Statistics s;
	char *relName[] = {"partsupp", "supplier", "nation"};

	s.Read(statisticsFile);

	s.AddRel(relName[0], 800000);
	s.AddAtt(relName[0], "ps_suppkey", 10000);

	s.AddRel(relName[1], 10000);
	s.AddAtt(relName[1], "s_suppkey", 10000);
	s.AddAtt(relName[1], "s_nationkey", 25);

	s.AddRel(relName[2], 25);
	s.AddAtt(relName[2], "n_nationkey", 25);
	s.AddAtt(relName[2], "n_name", 25);

	char *cnf = " (s_suppkey = ps_suppkey) ";
	yy_scan_string(cnf);
	yyparse();
	s.Apply(final, relName, 2);

	cnf = " (s_nationkey = n_nationkey)  AND (n_name = 'AMERICA')   ";
	yy_scan_string(cnf);
	yyparse();

	double result = s.Estimate(final, relName, 3);

	if (fabs(result - 32000) > 0.1)
		estimationFlag = false;
	s.Apply(final, relName, 3);

	s.Write(statisticsFile);

	ASSERT_TRUE(estimationFlag);
}

TEST(GtestForStatisticalEstimation_3, SubTest3)
{
	bool estimationFlag = true;

	Statistics s;
    char *relName[] = {"orders","customer","nation"};

	
	s.AddRel(relName[0],1500000);
	s.AddAtt(relName[0], "o_custkey",150000);

	s.AddRel(relName[1],150000);
	s.AddAtt(relName[1], "c_custkey",150000);
	s.AddAtt(relName[1], "c_nationkey",25);
	
	s.AddRel(relName[2],25);
	s.AddAtt(relName[2], "n_nationkey",25);

	char *cnf = "(c_custkey = o_custkey)";
	yy_scan_string(cnf);
	yyparse();

	// Join the first two relations in relName
	s.Apply(final, relName, 2);
	
	cnf = " (c_nationkey = n_nationkey)";
	yy_scan_string(cnf);
	yyparse();
	
	double result = s.Estimate(final, relName, 3);
	if(fabs(result-1500000)>0.1)
		estimationFlag = false;
	s.Apply(final, relName, 3);

	s.Write(statisticsFile);

	ASSERT_TRUE(estimationFlag);
}

TEST(GtestForStatisticalEstimation_4, SubTest4)
{
	bool estimationFlag = true;

	Statistics s;
	char *relName[] = {"supplier","customer","nation"};

	// s.Read(fileName);
	
	s.AddRel(relName[0],10000);
	s.AddAtt(relName[0], "s_nationkey",25);

	s.AddRel(relName[1],150000);
	s.AddAtt(relName[1], "c_custkey",150000);
	s.AddAtt(relName[1], "c_nationkey",25);
	
	s.AddRel(relName[2],25);
	s.AddAtt(relName[2], "n_nationkey",25);

	s.CopyRel("nation","n1");
	s.CopyRel("nation","n2");
	s.CopyRel("supplier","s");
	s.CopyRel("customer","c");

	char *set1[] ={"s","n1"};
	char *cnf = "(s.s_nationkey = n1.n_nationkey)";
	yy_scan_string(cnf);
	yyparse();	
	s.Apply(final, set1, 2);
	
	char *set2[] ={"c","n2"};
	cnf = "(c.c_nationkey = n2.n_nationkey)";
	yy_scan_string(cnf);
	yyparse();
	s.Apply(final, set2, 2);

	char *set3[] = {"c","s","n1","n2"};
	cnf = " (n1.n_nationkey = n2.n_nationkey )";
	yy_scan_string(cnf);
	yyparse();

	double result = s.Estimate(final, set3, 4);
	if(fabs(result-60000000.0)>0.1)
		estimationFlag = false;

	s.Apply(final, set3, 4);

	s.Write(statisticsFile);

	ASSERT_TRUE(estimationFlag);
}

int main(int argc, char **argv)
{
	std::cout << "Gtest for Statistical Estimation on DBFiles" << std::endl;
	testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}