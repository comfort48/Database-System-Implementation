#include<iostream>
#include<gtest/gtest.h>
#include "Pipe.h"
#include "Record.h"
#include "File.h"
#include"BigQ.h"

#define PIPE_SIZE 7000000

TEST(GTEST_1, SortedFileTestForNationTable) {
    
    std::cout<<"Gtest for checking whether the Nation DBFile is sorted or not"<<std::endl;

	Pipe inputPipe(PIPE_SIZE);
	Pipe outputPipe(PIPE_SIZE);

    Record tempRecord;

	FILE *dbFile = fopen("nation.tbl", "r");
	Schema testSchema("catalog", "nation");
	
    std::cout<<"Reading nation.tbl records to input pipe...."<<std::endl;

	while (tempRecord.SuckNextRecord(&testSchema, dbFile) == 1) {
		inputPipe.Insert(&tempRecord);
	}

	fclose(dbFile);
	inputPipe.ShutDown();

    std::cout<<"input pipe is filled with nation table records...."<<std::endl;

	ComparisonEngine comparator;
	OrderMaker sortorder(&testSchema);

    std::cout<<"Sorting nation table records...."<<std::endl;

	BigQ bq(inputPipe, outputPipe, sortorder, 12);
	bool isSorted = true;

	Record records[2];
	Record *lastRecord = NULL, *previousRecord = NULL;
	int counter = 0;

    std::cout<<"Completed Sorting input pipe records and pushed them to output pipe...."<<std::endl;
    
	while (outputPipe.Remove(&records[counter % 2])) {
        
		previousRecord = lastRecord;
		lastRecord = &records[counter % 2];

		if (lastRecord && previousRecord) {
			if (comparator.Compare(previousRecord, lastRecord, &sortorder) == 1) {
				isSorted = false;
                break;
			}

		}
		counter++;
	}

	ASSERT_TRUE(isSorted);
}

TEST(GTEST_2, SortedFileTestForCustomerTable) {
    
    std::cout<<"Gtest for checking whether the Customer DBFile is sorted or not"<<std::endl;

	Pipe inputPipe(PIPE_SIZE);
	Pipe outputPipe(PIPE_SIZE);

    Record tempRecord;

	FILE *dbFile = fopen("customer.tbl", "r");
	Schema testSchema("catalog", "customer");
	
    std::cout<<"Reading customer.tbl records to input pipe...."<<std::endl;

	while (tempRecord.SuckNextRecord(&testSchema, dbFile) == 1) {
		inputPipe.Insert(&tempRecord);
	}

	fclose(dbFile);
	inputPipe.ShutDown();

    std::cout<<"input pipe is filled with customer table records...."<<std::endl;

	ComparisonEngine comparator;
	OrderMaker sortorder(&testSchema);

    std::cout<<"Sorting customer table records...."<<std::endl;

	BigQ bq(inputPipe, outputPipe, sortorder, 12);
	bool isSorted = true;

	Record records[2];
	Record *lastRecord = NULL, *previousRecord = NULL;
	int counter = 0;

    std::cout<<"Completed Sorting input pipe records and pushed them to output pipe...."<<std::endl;
    
	while (outputPipe.Remove(&records[counter % 2])) {
        
		previousRecord = lastRecord;
		lastRecord = &records[counter % 2];

		if (lastRecord && previousRecord) {
			if (comparator.Compare(previousRecord, lastRecord, &sortorder) == 1) {
				isSorted = false;
                break;
			}

		}
		counter++;
	}

	ASSERT_TRUE(isSorted);
	
}

TEST(GTEST_3, SortedFileTestForPartTable) {
  
    std::cout<<"Gtest for checking whether the Part DBFile is sorted or not"<<std::endl;

	Pipe inputPipe(PIPE_SIZE);
	Pipe outputPipe(PIPE_SIZE);

    Record tempRecord;

	FILE *dbFile = fopen("part.tbl", "r");
	Schema testSchema("catalog", "part");
	
    std::cout<<"Reading part.tbl records to input pipe...."<<std::endl;

	while (tempRecord.SuckNextRecord(&testSchema, dbFile) == 1) {
		inputPipe.Insert(&tempRecord);
	}

	fclose(dbFile);
	inputPipe.ShutDown();

    std::cout<<"input pipe is filled with part table records...."<<std::endl;

	ComparisonEngine comparator;
	OrderMaker sortorder(&testSchema);

    std::cout<<"Sorting part table records...."<<std::endl;

	BigQ bq(inputPipe, outputPipe, sortorder, 12);
	bool isSorted = true;

	Record records[2];
	Record *lastRecord = NULL, *previousRecord = NULL;
	int counter = 0;

    std::cout<<"Completed Sorting input pipe records and pushed them to output pipe...."<<std::endl;
    
	while (outputPipe.Remove(&records[counter % 2])) {
        
		previousRecord = lastRecord;
		lastRecord = &records[counter % 2];

		if (lastRecord && previousRecord) {
			if (comparator.Compare(previousRecord, lastRecord, &sortorder) == 1) {
				isSorted = false;
                break;
			}

		}
		counter++;
	}

	ASSERT_TRUE(isSorted);
}

TEST(GTEST_4, SortedFileTestForLineItemTable) {

    std::cout<<"Gtest for checking whether the Lineitem DBFile is sorted or not"<<std::endl;

	Pipe inputPipe(PIPE_SIZE);
	Pipe outputPipe(PIPE_SIZE);

    Record tempRecord;

	FILE *dbFile = fopen("lineitem.tbl", "r");
	Schema testSchema("catalog", "lineitem");
	
    std::cout<<"Reading lineitem.tbl records to input pipe...."<<std::endl;

	while (tempRecord.SuckNextRecord(&testSchema, dbFile) == 1) {
		inputPipe.Insert(&tempRecord);
	}

	fclose(dbFile);
	inputPipe.ShutDown();

    std::cout<<"input pipe is filled with lineitem table records...."<<std::endl;

	ComparisonEngine comparator;
	OrderMaker sortorder(&testSchema);

    std::cout<<"Sorting lineitem table records...."<<std::endl;

	BigQ bq(inputPipe, outputPipe, sortorder, 12);
	bool isSorted = true;

	Record records[2];
	Record *lastRecord = NULL, *previousRecord = NULL;
	int counter = 0;

    std::cout<<"Completed Sorting input pipe records and pushed them to output pipe...."<<std::endl;
    
	while (outputPipe.Remove(&records[counter % 2])) {
        
		previousRecord = lastRecord;
		lastRecord = &records[counter % 2];

		if (lastRecord && previousRecord) {
			if (comparator.Compare(previousRecord, lastRecord, &sortorder) == 1) {
				isSorted = false;
                break;
			}

		}
		counter++;
	}

	ASSERT_TRUE(isSorted);
}

int main(int argc, char **argv) {

    std::cout<<"Gtest for sorted DBFile"<<std::endl;
	testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
    
}
