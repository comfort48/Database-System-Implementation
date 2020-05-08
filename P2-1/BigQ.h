#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include <vector>
using namespace std;


class BigQ {

private:
	Pipe *inputPipe;         // input Pipe to get records 
	Pipe *outputPipe;        // ouput Pipe to push records
	OrderMaker *sortedOrder; // sorted Order required for sorting
	int *runLength;          // the run length 
	File *runsFile;			 // file pointer for the runs file 
	vector<int> runPointers; // list of pointers to all the runs 
public:
	/* Contructor to intialize the fields of BigQ*/
	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	/* worker function which does the TPMMS sorting */
	void worker();
	/* start of the TPMMS Algorithm from the new thread */
	static void* invoke_tpmmsAlgo( void *args );
	/* sort the current Run */
	void sortRun( vector <Record*> &);
	/* write the current sorted Run to the File */
	int addRuntoFile(vector <Record*> & );
	~BigQ ();
};

#endif
