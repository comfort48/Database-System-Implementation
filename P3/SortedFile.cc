#include "SortedFile.h"
#include "Comparison.h"
#include <fstream>
#include <string.h>
#include "Heap.h"

/*Constructor*/
SortedFile::SortedFile(/* args */)
{
    this->fileBuffer = new File();     // file instance
    this->writeBuffer = new Page();    // page instance, write buffer
    this->readBuffer = new Page();     // page instance, read buffer
    this->isEmptyWriteBuffer = true;   // to check if write buffer is empty
    this->endoffile = false;           // to check end of file has reached
    this->pageIdx = 0;                 // setting the pageIdx intially to zero
    this->sortedFileMode = R;          // initially mode of file is set to Reading
    this->bigQ = NULL;                 // BigQ is kept NULL initially
    this->inputPipe = new Pipe(1000);  // new inputpipe is created with size 1000
    this->outputPipe = new Pipe(1000); // new ouputpipe is created with size 1000
    this->queryOrderMaker = NULL;      // query Order Maker used for GetNext
    this->isQueryChanged = true;       // initial query change is set to true
}

/*Constructor to intialise the sorted Order*/
SortedFile::SortedFile(OrderMaker *sortOrder, int runlen)
{
    this->fileBuffer = new File();   // file instance
    this->writeBuffer = new Page();  // page instance, write buffer
    this->readBuffer = new Page();   // page instance, read buffer
    this->isEmptyWriteBuffer = true; // to check if write buffer is empty
    this->endoffile = false;         // to check end of file has reached
    this->pageIdx = 0;               // setting the pageIdx intially to zero
    this->sortInfo = new SortInfo(); // setting the sorted order and runlen
    this->sortInfo->myOrder = sortOrder;
    this->sortInfo->runLength = runlen;
    this->sortedFileMode = R;          // initially mode of file is set to Reading
    this->bigQ = NULL;                 // BigQ is kept NULL initially
    this->inputPipe = new Pipe(1000);  // new inputpipe is created with size 1000
    this->outputPipe = new Pipe(1000); // new ouputpipe is created with size 1000
    this->queryOrderMaker = NULL;      // query Order Maker used for GetNext
    this->isQueryChanged = true;       // initial query change is set to true
}

int SortedFile::Create(char *f_path, fType f_type, void *startup)
{
    file_path = f_path;
    char metadata[100];
    strcpy(metadata, f_path);
    strcat(metadata, "-metadata.header");
    // initially keeping the flag as true for write Buffer
    this->isEmptyWriteBuffer = true;
    ofstream fileOutputStream;
    fileOutputStream.open(metadata);
    // f_type is added to metadata file
    SortInfo *varSortInfo = ((SortInfo *)startup);
    fileOutputStream << "sorted" << endl;
    fileOutputStream << varSortInfo->runLength << endl;
    varSortInfo->myOrder->Print_metadata(fileOutputStream);
    // Open operation on the File is called
    fileBuffer->Open(0, (char *)f_path);
    fileOutputStream.close();
    return 1;
}

int SortedFile::Open(char *fpath)
{
    file_path = fpath;
    //Open function from File class on the existing bin file created
    this->fileBuffer->Open(1, (char *)fpath);
    //initially keep the end of file as false and page Index to zero
    this->endoffile = false;
    this->pageIdx = 0;
    return 1;
}

int SortedFile::Close()
{
    if (sortedFileMode == W)
    {
        sortedFileMode = R;
        this->Merge();
    }
    fileBuffer->Close();

    return 1;
}

void SortedFile::Load(Schema &myschema, char *loadpath)
{

    if (sortedFileMode == R)
    {
        sortedFileMode = W;
        inputPipe = new Pipe(1000);
        outputPipe = new Pipe(1000);
        bigQ = new BigQ(*inputPipe, *outputPipe, *sortInfo->myOrder, sortInfo->runLength);
    }

    FILE *fileInputStream = fopen(loadpath, "r");
    Record currRecord;
    // records are read using SuckNextRecord function from Record.h
    while (currRecord.SuckNextRecord(&myschema, fileInputStream) == 1)
    {
        // Add function is called to add the record to DBFile instance
        inputPipe->Insert(&currRecord);
    }
    fclose(fileInputStream);
}

void SortedFile::MoveFirst()
{

    if (sortedFileMode == W)
    {
        this->Merge();
        sortedFileMode = R;
    }

    // On existing file we are extracting the first page into read Buffer
    this->fileBuffer->GetPage(this->readBuffer, 0);
    this->isQueryChanged = true;
}

void SortedFile::Add(Record &addme)
{

    if (this->sortedFileMode == R)
    {

        this->sortedFileMode = W;
        this->inputPipe = new Pipe(100);
        this->outputPipe = new Pipe(100);
        this->bigQ = new BigQ(*inputPipe, *outputPipe, *sortInfo->myOrder, sortInfo->runLength);
    }
    inputPipe->Insert(&addme);
    this->isQueryChanged = true;
}

/*2 way merge function*/
void SortedFile::Merge()
{

    // shutDown the input pipe
    inputPipe->ShutDown();

    Heap *mergeFile = new Heap();
    mergeFile->Create("mergefile.bin", heap, NULL);

    Record *currRec = new Record();
    Record *currRecPQ = new Record();
    readBuffer->EmptyItOut();

    // first case where the sorted file is empty
    if (fileBuffer->GetLength() == 0)
    {
        while (outputPipe->Remove(currRecPQ))
        {
            mergeFile->Add(*currRecPQ);
        }
    }
    // case for merging the sorted file with the output pipe from BigQ
    else
    {
        fileBuffer->GetPage(readBuffer, 0);
        pageIdx = 1;
        ComparisonEngine cmp;
        int pipeResult = outputPipe->Remove(currRecPQ);
        int fileResult = readBuffer->GetFirst(currRec);

        while (fileResult && pipeResult)
        {

            if (cmp.Compare(currRec, currRecPQ, sortInfo->myOrder) > 0)
            {
                mergeFile->Add(*currRecPQ);
                pipeResult = outputPipe->Remove(currRecPQ);
            }
            else if (cmp.Compare(currRec, currRecPQ, sortInfo->myOrder) <= 0)
            {
                mergeFile->Add(*currRec);
                fileResult = readBuffer->GetFirst(currRec);

                if (!fileResult && pageIdx < (fileBuffer->GetLength() - 1))
                {
                    delete readBuffer;
                    readBuffer = new Page();
                    fileBuffer->GetPage(readBuffer, pageIdx);
                    fileResult = readBuffer->GetFirst(currRec);
                    pageIdx++;
                }
            }
        }
        while (pipeResult)
        {
            mergeFile->Add(*currRecPQ);
            pipeResult = outputPipe->Remove(currRecPQ);
        }
        while (fileResult)
        {
            mergeFile->Add(*currRec);
            fileResult = readBuffer->GetFirst(currRec);

            if (!fileResult && pageIdx < (fileBuffer->GetLength() - 1))
            {
                delete readBuffer;
                readBuffer = new Page();
                fileBuffer->GetPage(readBuffer, pageIdx);
                fileResult = readBuffer->GetFirst(currRec);
                pageIdx++;
            }
        }
    }
    mergeFile->Close();

    fileBuffer->Close();
    // rename the mergefile as the sortedbin and reopen it for further merging
    rename("mergefile.bin", file_path);
    this->Open(file_path);
    delete inputPipe;
    delete outputPipe;
    delete bigQ;
    inputPipe = NULL;
    outputPipe = NULL;
    bigQ = NULL;
}

int SortedFile::GetNext(Record &fetchme)
{
    if (sortedFileMode == W)
    {
        sortedFileMode = R;
        this->Merge();
    }
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

int SortedFile::GetNext(Record &fetchme, CNF &cnf, Record &literal)
{

    if (sortedFileMode == W)
    {
        sortedFileMode = R;
        this->Merge();
    }
    ComparisonEngine cmpEng;

    // checking if the query has changes from the sorted Order
    OrderMaker queryOrder, queryOrderForLiteral;
    // building the QueryOrderMaker by using the sorted order of file and the CNF order
    OrderMaker::makeQueryOrderMaker(sortInfo->myOrder, cnf, queryOrder, queryOrderForLiteral); // make QueryOrders
    // queryOrderMaker needed built from the sorted Order
    queryOrderMaker = cnf.getQueryOrderMaker(*(sortInfo->myOrder)); // storeQueryOrderMaker for future use when we get the same cnf next time
    // perform binary search to fetch the records matching the queryOrderMaker
    if (!binarySearch(fetchme, &queryOrder, literal, &queryOrderForLiteral, cmpEng))
    {
        return 0;
    }
    //continue searching records based on queryOrderMaker
    do
    {
        /*if (cmpEng.Compare(&fetchme, queryOrderMaker, &literal, sortInfo->myOrder) != 0)
                    return 0;*/
        if (cmpEng.Compare(&fetchme, &literal, &cnf))
            return 1;
    } while (GetNext(fetchme));

    return 0;
}

/*Binary Search performed over the sorted File*/
int SortedFile::binarySearch(Record &fetchme, OrderMaker *queryOrder, Record &literal, OrderMaker *queryOrderForLiteral, ComparisonEngine &cmpEng)
{

    if (!GetNext(fetchme))
        return 0;

    int resultFlag = cmpEng.Compare(&fetchme, queryOrder, &literal, queryOrderForLiteral);

    if (resultFlag == 0)
        return 1; //  Found

    if (resultFlag > 0)
        return 0;

    int lowPageIndex = pageIdx, highPageIndex = fileBuffer->GetLength() - 1, midPageIndex = (lowPageIndex + highPageIndex) / 2;

    while (lowPageIndex < midPageIndex)
    {

        midPageIndex = (lowPageIndex + highPageIndex) / 2;
        fileBuffer->GetPage(readBuffer, midPageIndex);
        pageIdx = midPageIndex + 1;
        if (!GetNext(fetchme))
            printf("No Record to be fetched");
        resultFlag = cmpEng.Compare(&fetchme, queryOrder, &literal, queryOrderForLiteral);
        if (resultFlag < 0)
            lowPageIndex = midPageIndex;
        else if (resultFlag > 0)
            highPageIndex = midPageIndex - 1;
        else
            highPageIndex = midPageIndex;
    }
    fileBuffer->GetPage(readBuffer, lowPageIndex);
    pageIdx = lowPageIndex + 1;
    // continue fetches the records from binary search point
    do
    {
        if (!GetNext(fetchme))
            return 0;
        resultFlag = cmpEng.Compare(&fetchme, queryOrder, &literal, queryOrderForLiteral);
    } while (resultFlag < 0);

    return ((resultFlag == 0) ? 1 : 0);
}

SortedFile::~SortedFile()
{
}
