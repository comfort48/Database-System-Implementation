#include <gtest/gtest.h>
#include "DBFile.h"

DBFile dbfile1;
string filePath1 = "lineitem.bin";

TEST(DBFileCreation, CreateFile_LineItem) {

    cout<<"Creating lineitem.bin"<<endl;
    ASSERT_EQ(1, dbfile1.Create(filePath1.c_str(), heap, NULL));

}

TEST(DBFileOpen, OpenFile_LineItem) {

    cout<<"Opening lineitem.bin"<<endl;
    ASSERT_EQ(1, dbfile1.Open(filePath1.c_str()));

}

TEST(DBFileClose, CloseFile_LineItem) {

    cout<<"Closing lineitem.bin"<<endl;
    ASSERT_EQ(1, dbfile1.Close());
    remove("lineitem.bin");
    remove("lineitem.bin-metadata.header");

}

DBFile dbfile2;
string filePath2 = "orders.bin";

TEST(DBFileCreation, CreateFile_Orders) {

    cout<<"Creating orders.bin"<<endl;
    ASSERT_EQ(1, dbfile2.Create(filePath2.c_str(), heap, NULL));

}

TEST(DBFileOpen, OpenFile_Orders) {
    
    cout<<"Opening orders.bin"<<endl;
    ASSERT_EQ(1, dbfile2.Open(filePath2.c_str()));

}

TEST(DBFileClose, CloseFile_Orders) {
    
    cout<<"Closing orders.bin"<<endl;
    ASSERT_EQ(1, dbfile2.Close());
    remove("orders.bin");
    remove("orders.bin-metadata.header");

}

DBFile dbfile3;
string filePath3 = "customer.bin";

TEST(DBFileCreation, CreateFile_Customer) {

    cout<<"Creating customer.bin"<<endl;
    ASSERT_EQ(1, dbfile3.Create(filePath3.c_str(), heap, NULL));

}

TEST(DBFileOpen, OpenFile_Customer) {
    
    cout<<"Opening customer.bin"<<endl;
    ASSERT_EQ(1, dbfile3.Open(filePath3.c_str()));

}

TEST(DBFileClose, CloseFile_Customer) {

    cout<<"Closing customer.bin"<<endl;
    ASSERT_EQ(1, dbfile3.Close());
    remove("customer.bin");
    remove("customer.bin-metadata.header");

}

int main(int argc, char *argv[]) {
    
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();

}
