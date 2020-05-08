#ifndef REL_OP_H
#define REL_OP_H

#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"
#include <sstream>
#include "BigQ.h"

class RelationalOp
{
public:
	// blocks the caller until the particular relational operator
	// has run to completion
	virtual void WaitUntilDone() = 0;

	// tell us how much internal memory the operation can use
	virtual void Use_n_Pages(int n) = 0;
};

//Arugments are binded into a structure 
struct ThreadArgs
{

public:
	Pipe *inputPipe, *outputPipe;
	CNF *cnf;
	DBFile *dbFile;
	Record *recordliteral;
	int *keepMe;
	int numAttsInput;
	int numAttsOutput;
	Function *computeMe;
	Schema *mySchema;
	FILE *writeOutFile;
	OrderMaker *groupAtts;
};

typedef struct ThreadArgs workerArgs_t;

class SelectFile : public RelationalOp
{

private:
	pthread_t thread;
	Record *buffer;
	int runLength;

public:
	void Run(DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone();
	void Use_n_Pages(int n);
};

class SelectPipe : public RelationalOp
{

private:
	pthread_t thread;
	int runLength;

public:
	void Run(Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone();
	void Use_n_Pages(int n);
};

class Project : public RelationalOp
{

private:
	pthread_t thread;
	int runLength;

public:
	void Run(Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput);
	void WaitUntilDone();
	void Use_n_Pages(int n);
};

class Join : public RelationalOp
{

private:
	pthread_t thread;
	int runLength;

	Pipe *inputPipe_L;
	Pipe *inputPipe_R;
	Pipe *outputPipe;
	CNF *inputCNF;
	Record *inputliteral;

public:
	void Run(Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone();
	void Use_n_Pages(int n);
	static void *JoinOperation(void *args);
	bool checkConsecutiveRecords(Pipe &pipe, Record *rec, vector<Record *> &vec, OrderMaker *sortOrder);
	void mergeLeftRightrecords(Record *l, Record *r);
};

class DuplicateRemoval : public RelationalOp
{
private:
	pthread_t thread;
	int runLength;

public:
	void Run(Pipe &inPipe, Pipe &outPipe, Schema &mySchema);
	void WaitUntilDone();
	void Use_n_Pages(int n);
};

class Sum : public RelationalOp
{

private:
	pthread_t thread;
	int runLength;

public:
	void Run(Pipe &inPipe, Pipe &outPipe, Function &computeMe);
	void WaitUntilDone();
	void Use_n_Pages(int n);
};

class GroupBy : public RelationalOp
{
private:
	pthread_t thread;
	int runLength;

	OrderMaker *inputOrder;
	Function *inputFunc;

	Pipe *inputPipe;
	Pipe *outputPipe;

public:
	void Run(Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe);
	void WaitUntilDone();
	void Use_n_Pages(int n);

	static void *GroupByOperation(void *args);
	Type aggregateSum(Record *rec, int &isum, double &dsum);
	void createRecordwithGrpSum(Record *rec, Type type, int isum, double dsum);
};

class WriteOut : public RelationalOp
{

private:
	pthread_t thread;
	int runLength;

public:
	void Run(Pipe &inPipe, FILE *outFile, Schema &mySchema);
	void WaitUntilDone();
	void Use_n_Pages(int n);
};
#endif
