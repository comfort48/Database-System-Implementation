#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include "string.h"
#include "fstream"
#include<stdio.h>

// stub file .. replace it with your own DBFile.cc

/*Constructor*/
DBFile::DBFile () {
    this->fileBuffer = new File();  // file instance 
    this->writeBuffer = new Page(); // page instance, write buffer 
    this->readBuffer = new Page();  // page instance, read buffer
    this->isEmptyWriteBuffer = true;// to check if write buffer is empty
    this->endoffile = false;        // to check end of file has reached
    this->pageIdx = 0;              // setting the pageIdx intially to zero
}

/*Create normal file*/
int DBFile::Create (const char *f_path, fType f_type, void *startup) {
    char metadata[100];
    strcpy( metadata , f_path);
    strcat( metadata , "-metadata.header");
    // initially keeping the flag as true for write Buffer
    this->isEmptyWriteBuffer = true;
    ofstream fileOutputStream;
    fileOutputStream.open(metadata);

    // f_type is added to metadata file
    if ( f_type == heap ){
        fileOutputStream << "Heap File Created\n";
    }
    // Open operation on the File is called
    fileBuffer->Open(0, (char *)f_path);  
    fileOutputStream.close();
    return 1; // return value 1 on success
}

/*load the DBFile instance .tbl files*/
void DBFile::Load (Schema &f_schema, const char *loadpath) {
    FILE  *fileInputStream = fopen(loadpath , "r");
    Record currRecord;
    // records are read using SuckNextRecord function from Record.h
    while( currRecord.SuckNextRecord( &f_schema , fileInputStream ) == 1 ){
        // Add function is called to add the record to DBFile instance
        this->Add(currRecord);
    }
    fclose(fileInputStream);
}

/*Assumes Heap file exists and DBFile instance points to the Heap file */
int DBFile::Open (const char *f_path) {
    //Open function from File class on the existing bin file created
    this->fileBuffer->Open(1, (char *)f_path);
    //initially keep the end of file as false and page Index to zero
    this->endoffile = false;
    this->pageIdx = 0;
    return 1;
}

/*Method points to the first record in the file*/
void DBFile::MoveFirst () {
    // On existing file we are extracting the first page into read Buffer
    this->fileBuffer->GetPage(this->readBuffer, 0);
}

/*Closes the file */
int DBFile::Close () {
    // checking if the write buffer is empty to write the last page to file
    if( this->isEmptyWriteBuffer == false ){
        off_t no_of_pages = fileBuffer->GetLength();
        if( no_of_pages != 0  ){
            no_of_pages = no_of_pages - 1;
        }
        this->fileBuffer->AddPage(writeBuffer, no_of_pages );
        writeBuffer->EmptyItOut();
    }
    this->fileBuffer->Close();
    // delete all the objects instances
    delete fileBuffer;
    delete writeBuffer;
    delete readBuffer;
    isEmptyWriteBuffer = true;

    return 1; // returns 1 on success file closed
}

/*add new record to end of file*/
void DBFile::Add (Record &rec) {
    Record write;
    write.Consume(&rec);
    off_t no_of_pages = (*fileBuffer).GetLength();

    this->isEmptyWriteBuffer = false;
    if(  writeBuffer->Append(&write) == 0 ){
    // Append operation failed
        if( no_of_pages == 0 ){
            // first page adding to file
            fileBuffer->AddPage(writeBuffer , 0);
        }
        else{
            // adding page to file
            fileBuffer->AddPage(writeBuffer , no_of_pages-1); 
        }
    // empty the write buffer
        writeBuffer->EmptyItOut();
    // append the record to the write buffer
        writeBuffer->Append(&write);
    }
}

/* gets the next record present in heap file and returns */
int DBFile::GetNext (Record &fetchme) {
    // checking end of file 
    if( this->endoffile == false ){
        // extracting the next record from readbuffer
        int result = this->readBuffer->GetFirst(&fetchme);
        // checking if the readbuffer has any records 
        if( result == 0 ){
            // incrementing the page index
            pageIdx++;
            // checking if the end of file has reached
            if( pageIdx >= this->fileBuffer->GetLength()-1 ){
                // updating end of file as true
                this->endoffile = true;
                return 0; // return 0 as end of file has reached
            }else{
                // extracting next page from file into read buffer
                this->fileBuffer->GetPage(this->readBuffer,pageIdx);
                this->readBuffer->GetFirst(&fetchme);
            }
        }
        return 1; // return 1 on success, next record is fed into fetchme
    }
    return 0; // return 0 as end of file has reached
}

/*gets the next record from the Heap file if the accepted by the predicate*/
int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {

    ComparisonEngine cmpengine;

    while( GetNext(fetchme) ){
        if( cmpengine.Compare(&fetchme,&literal,&cnf) )
            return 1;  // return 1 as there is record accepted by the predicate
    }

    return 0; // return 0 end of file reached and no more accepted by predicate 
}
