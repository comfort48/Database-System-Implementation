#ifndef HEAP_H
#define HEAP_H

#include "Schema.h"
#include "Record.h"
#include <string.h>
#include "GenericDBFile.h"

class Heap : virtual public GenericDBFile
{
public:
    /* data */
    File *fileBuffer;        // file instance
    Page *writeBuffer;       // page instance, write buffer
    Page *readBuffer;        // page instance, read buffer
    bool isEmptyWriteBuffer; // to check if write buffer is empty
    bool endoffile;          // to check end of file has reached
    int pageIdx;             // to number of pages on file
public:
    Heap(/* args */);
    /*Create Heap file*/
    int Create(char *fpath, fType file_type, void *startup);
    /*Opens Heap file*/
    int Open(char *fpath);
    /*Closes the Heap file*/
    int Close();
    /*load the .tbl files into Heap*/
    void Load(Schema &myschema, char *loadpath);
    /*points to first record in the file*/
    void MoveFirst();
    /*add new record to end of file*/
    void Add(Record &addme);
    /*gets the next record present in file and returns*/
    int GetNext(Record &fetchme);
    /*return the next record in the file accepted by predicate*/
    int GetNext(Record &fetchme, CNF &cnf, Record &literal);
    // int getLength();
    ~Heap();
};

#endif