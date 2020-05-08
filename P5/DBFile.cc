#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include<fstream>
#include "Pipe.h"
#include "BigQ.h"
#include "Heap.h"
#include "SortedFile.h"

// stub file .. replace it with your own DBFile.cc

/* Constructor */
DBFile::DBFile()
{
	this->myInternalVar = NULL;
}

/*Create function, internal variable create Heap or SortedFile object*/
int DBFile::Create(char *f_path, fType f_type, void *startup)
{
	if (f_type == heap)
	{
		myInternalVar = new Heap();
	}
	else if (f_type == sorted)
	{
		myInternalVar = new SortedFile();
	}
	return myInternalVar->Create(f_path, f_type, startup);
}

/*Load the file based on the loadpath*/
void DBFile::Load(Schema &f_schema, char *loadpath)
{
	myInternalVar->Load(f_schema, loadpath);
}

/*Open Function which points to Heap or sorted File using metadata*/
int DBFile::Open(char *f_path)
{
	char metadata[100];
	strcpy(metadata, f_path);
	strcat(metadata, ".header");
	ifstream fileInputStream;
	fileInputStream.open(metadata);
	string line;
	getline(fileInputStream, line);
	if ( stoi(line) == 0)
	{
		myInternalVar = new Heap();
		return myInternalVar->Open(f_path);

	}
	else if ( stoi(line) == 1)
	{
		getline(fileInputStream, line);
		int runLength = stoi(line);
		OrderMaker *sortOrder = new OrderMaker(fileInputStream);
		myInternalVar = new SortedFile(sortOrder, runLength);
		return myInternalVar->Open(f_path);
	}
	fileInputStream.close();
	return 0;
}

/*function that points to the first record in the bin file*/
void DBFile::MoveFirst()
{
	myInternalVar->MoveFirst();
}

/*function that closes the existing bin file*/
int DBFile::Close()
{
	myInternalVar->Close();
}

/*function which adds the record to the coming bin file*/
void DBFile::Add(Record &rec)
{
	myInternalVar->Add(rec);
}

/*function which returns the next record in file*/
int DBFile::GetNext(Record &fetchme)
{
	return myInternalVar->GetNext(fetchme);
}

/*function which returns the next record which satisfies the predicate */
int DBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal)
{
	return myInternalVar->GetNext(fetchme, cnf, literal);
}
