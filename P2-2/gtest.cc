#include<gtest/gtest.h>
#include "DBFile.h"
#include "SortedFile.h"

#define PIPE_SIZE 7000000

DBFile dbfile1;
string filePath1 = "lineitem.bin";
const char *tpch_dir ="/home/aseesh/git/tpch-dbgen/nation.tbl";

TEST(DBFileCreation, CreateFile_LineItem) {

    OrderMaker *om = new OrderMaker();
    struct
	{
		OrderMaker *o;
		int l;
	} startup = {om, 48};

    cout<<"Creating lineitem.bin"<<endl;
    ASSERT_EQ(1, dbfile1.Create((char *)filePath1.c_str(), sorted, &startup));

}

TEST(DBFileOpen, OpenFile_LineItem) {

    cout<<"Opening lineitem.bin"<<endl;
    ASSERT_EQ(1, dbfile1.Open((char *)filePath1.c_str()));

}

TEST(DBFileClose, CloseFile_LineItem) {

    cout<<"Closing lineitem.bin"<<endl;
    ASSERT_EQ(1, dbfile1.Close());
    remove("lineitem.bin");
    remove("lineitem.bin-metadata.header");

}

TEST(SortedFileTest, SortedFileTestForNationTable) {
  
    std::cout<<"Gtest for checking whether the Nation DBFile is sorted or not"<<std::endl;

	Pipe inputPipe(PIPE_SIZE);
	Pipe outputPipe(PIPE_SIZE);

    Record tempRecord;

	FILE *dbFile = fopen(tpch_dir, "r");
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

	BigQ bq(inputPipe, outputPipe, sortorder, 10000);
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



