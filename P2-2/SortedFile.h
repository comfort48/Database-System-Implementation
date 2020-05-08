#ifndef SORTEDFILE_H
#define SORTEDFILE_H

#include "GenericDBFile.h"
#include "BigQ.h"

struct SortInfo
{
    OrderMaker *myOrder;
    int runLength;
};

enum Mode { R, W };

class SortedFile : virtual public GenericDBFile
{
private:
    /* data */
    File *fileBuffer;          // file instance
    Page *writeBuffer;         // page instance, write buffer
    Page *readBuffer;          // page instance, read buffer
    bool isEmptyWriteBuffer;   // to check if write buffer is empty
    bool endoffile;            // to check end of file has reached
    int pageIdx;               // to number of pages on file
    SortInfo *sortInfo;         // sort information
    Mode sortedFileMode;    // mode of the sorted file
    BigQ *bigQ;             //bigQ object used for sorting 
    Pipe *inputPipe;        // input pipe needed by BigQ
    Pipe *outputPipe;       // output pipe needed by BigQ
    char *file_path;        // current name and path of the file
    OrderMaker *queryOrderMaker;
    bool isQueryChanged;
public:
    SortedFile(/* args */);
    SortedFile( OrderMaker *sortedOrder, int runlen );
    int Create(char *fpath, fType file_type, void *startup);
    int Open(char *fpath);
    int Close();

    void Load(Schema &myschema, char *loadpath);
    void Merge();
    void MoveFirst();
    void Add(Record &addme);
    int GetNext(Record &fetchme);
    int GetNext(Record &fetchme, CNF &cnf, Record &literal);
    int binarySearch(Record& fetchme, OrderMaker* qorder, Record& literal, OrderMaker* qorder1, ComparisonEngine& cmp);
    //Record* GetMatchedPageRecord(Record &literal);
    //int binarySearch(int low, int high, OrderMaker *queryOM, Record &literal);
    ~SortedFile();
};

#endif