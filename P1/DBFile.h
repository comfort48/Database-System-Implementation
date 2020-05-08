#ifndef DBFILE_H
#define DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"

typedef enum {heap, sorted, tree} fType;

// stub DBFile header..replace it with your own DBFile.h 

class DBFile {

private:
	File *fileBuffer; // file instance 
	Page *writeBuffer; // page instance, write buffer 
	Page *readBuffer;	// page instance, read buffer
	bool isEmptyWriteBuffer; // to check if write buffer is empty
	bool endoffile;			// to check end of file has reached
	int pageIdx;			// to number of pages on file
public:
	/*Constructor*/
	DBFile (); 
	/*Create normal file*/
	int Create (const char *fpath, fType file_type, void *startup);
	/*Assumes Heap file exists and DBFile instance points to the Heap file */
	int Open (const char *fpath);
	/*Closes the file */
	int Close ();
	/*load the DBFile instance .tbl files*/
	void Load (Schema &myschema, const char *loadpath);
	/*Method points to the first record in the file*/
	void MoveFirst ();
	/*add new record to end of file*/
	void Add (Record &addme);
	/* gets the next record present in heap file and returns */
	int GetNext (Record &fetchme);
	/*gets the next record from the Heap file if the accepted by the predicate*/
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);

};
#endif
