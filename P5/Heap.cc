
#include "Heap.h"
#include <fstream>
#include <iostream>

Heap::Heap(/* args */)
{
    this->fileBuffer = new File();   // file instance
    this->writeBuffer = new Page();  // page instance, write buffer
    this->readBuffer = new Page();   // page instance, read buffer
    this->isEmptyWriteBuffer = true; // to check if write buffer is empty
    this->endoffile = false;         // to check end of file has reached
    this->pageIdx = 0;               // setting the pageIdx intially to zero
}

int Heap::Create(char *f_path, fType f_type, void *startup)
{
    char metadata[100];
    strcpy(metadata, f_path);
    strcat(metadata, ".header");
    // initially keeping the flag as true for write Buffer
    this->isEmptyWriteBuffer = true;
    ofstream fileOutputStream;
    fileOutputStream.open(metadata);
    // f_type is added to metadata file
    fileOutputStream << "0" << endl;
    // Open operation on the File is called
    fileBuffer->Open(0, (char *)f_path);
    fileOutputStream.close();
    return 1;
}

void Heap::Load(Schema &f_schema, char *loadpath)
{
    FILE *fileInputStream = fopen(loadpath, "r");
    Record currRecord;
    // records are read using SuckNextRecord function from Record.h
    while (currRecord.SuckNextRecord(&f_schema, fileInputStream) == 1)
    {
        // Add function is called to add the record to DBFile instance
        this->Add(currRecord);
    }
    fclose(fileInputStream);
}

int Heap::Open(char *f_path)
{
    //Open function from File class on the existing bin file created
    this->fileBuffer->Open(1, (char *)f_path);
    //initially keep the end of file as false and page Index to zero
    this->endoffile = false;
    this->isEmptyWriteBuffer = true;
    this->pageIdx = 0;
    return 1;
}

void Heap::MoveFirst()
{
    // On existing file we are extracting the first page into read Buffer
    this->fileBuffer->GetPage(this->readBuffer, 0);
}

int Heap::Close()
{
    // checking if the write buffer is empty to write the last page to file
    if (this->isEmptyWriteBuffer == false)
    {
        off_t no_of_pages = fileBuffer->GetLength();
        if (no_of_pages != 0)
        {
            no_of_pages = no_of_pages - 1;
        }
        this->fileBuffer->AddPage(writeBuffer, no_of_pages);
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

void Heap::Add(Record &rec)
{
    Record write;
    write.Consume(&rec);
    off_t no_of_pages = (*fileBuffer).GetLength();

    this->isEmptyWriteBuffer = false;
    if (writeBuffer->Append(&write) == 0)
    {
        // Append operation failed
        if (no_of_pages == 0)
        {
            // first page adding to file
            fileBuffer->AddPage(writeBuffer, 0);
        }
        else
        {
            // adding page to file
            fileBuffer->AddPage(writeBuffer, no_of_pages - 1);
        }
        // empty the write buffer
        writeBuffer->EmptyItOut();
        // append the record to the write buffer
        writeBuffer->Append(&write);
    }
}

int Heap::GetNext(Record &fetchme)
{
    // checking end of file
    if (this->endoffile == false)
    {
        // extracting the next record from readbuffer
        int result = this->readBuffer->GetFirst(&fetchme);
        // checking if the readbuffer has any records
        if (result == 0)
        {
            // incrementing the page index
            pageIdx++;
            // checking if the end of file has reached
            if (pageIdx >= this->fileBuffer->GetLength() - 1)
            {
                // updating end of file as true
                this->endoffile = true;
                return 0; // return 0 as end of file has reached
            }
            else
            {
                // extracting next page from file into read buffer
                this->fileBuffer->GetPage(this->readBuffer, pageIdx);
                this->readBuffer->GetFirst(&fetchme);
            }
        }
        return 1; // return 1 on success, next record is fed into fetchme
    }
    return 0; // return 0 as end of file has reached
}

int Heap::GetNext(Record &fetchme, CNF &cnf, Record &literal)
{
    ComparisonEngine cmpengine;

    while (GetNext(fetchme))
    {
        if (cmpengine.Compare(&fetchme, &literal, &cnf))
            return 1; // return 1 as there is record accepted by the predicate
    }

    return 0; // return 0 end of file reached and no more accepted by predicate
}

Heap::~Heap()
{
}