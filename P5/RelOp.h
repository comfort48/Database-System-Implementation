#ifndef REL_OP_H
#define REL_OP_H

#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"
#include <pthread.h>
//#include <string>
#include <sstream>
#include <unordered_map>

class RelationalOp {
	public:
	// blocks the caller until the particular relational operator 
	// has run to completion
	virtual void WaitUntilDone () = 0;

	// tell us how much internal memory the operation can use
	virtual void Use_n_Pages (int n) = 0;
};

class SelectFile : public RelationalOp { 
	static void *selectFileWorker(void *);
	private:
	pthread_t thread;  //the thread that run method will spawn
	int nPages; //the buffer that the operator can use
	DBFile *dbFile;
	Pipe *outputPipe;
	CNF *cnf;
	Record *recordliteral;
	// void DoSelectFile();
	public:

	void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);

};

class SelectPipe : public RelationalOp {
	static void *selectPipeWorker(void *);
	private:
	pthread_t thread;  //the thread that run method will spawn
	int nPages; //the buffer that the operator can use
	Pipe *inputPipe, *outputPipe;
	CNF *cnf;
	Record *recordliteral;
	// void DoSelectPipe();
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};


class Project : public RelationalOp { 
	static void *projectWorker(void *);
	private:
	pthread_t thread;  //the thread that run method will spawn
	int nPages; //the buffer that the operator can use
	Pipe *inputPipe, *outputPipe;
	int *keepMe;
	int numAttsInput, numAttsOutput;
	void DoProject();
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) ;
	void WaitUntilDone () ;
	void Use_n_Pages (int n) ;
};

class Join : public RelationalOp { 
	static void *join(void *);
	void DoJoin();
	private:
	pthread_t thread;  //the thread that run method will spawn
	int nPages; //the buffer that the operator can use
	Pipe *inputPipe_L, *inputPipe_R, *outputPipe;
	CNF *inputCNF;
	Record *inputliteral;

	public:
	void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone () ;
	void Use_n_Pages (int n) ;
	bool checkConsecutiveRecords(Pipe &pipe, Record *rec, vector<Record *> &vec, OrderMaker *sortOrder);
	void mergeLeftRightrecords(Record *l, Record *r);
};
class DuplicateRemoval : public RelationalOp {
	static void *duplicateRemoval(void *);
	private:
	pthread_t thread;  //the thread that run method will spawn
	int nPages; //the buffer that the operator can use
	Pipe *inputPipe, *outputPipe;
	Schema *mySchema;
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema) ;
	void WaitUntilDone () ;
	void Use_n_Pages (int n) ;
};
class Sum : public RelationalOp {
	static void *SumWorker(void *);
	private:
	pthread_t thread;  //the thread that run method will spawn
	int nPages; //the buffer that the operator can use
	Pipe *inputPipe, *outputPipe;
	Function *computeMe;
	void DoSum();
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe) ;
	void WaitUntilDone () ;
	void Use_n_Pages (int n) ;
};
class GroupBy : public RelationalOp {
	static void *groupBy(void *);
	private:
	pthread_t thread;  //the thread that run method will spawn
	int nPages; //the buffer that the operator can use
	Pipe *inputPipe, *outputPipe;
	OrderMaker *inputOrder;
	Function *inputFunc;
	void DoGroupBy();
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe) ;
	void WaitUntilDone () ;
	void Use_n_Pages (int n) ;
	Type aggregateSum(Record *rec, int &isum, double &dsum);
	void createRecordwithGrpSum(Record *rec, Type type, int isum, double dsum);
};
class WriteOut : public RelationalOp {
	static void *writeOutWorker(void *);
	private:
	pthread_t thread;  //the thread that run method will spawn
	int nPages; //the buffer that the operator can use
	Pipe *inputPipe;
	FILE *writeOutFile;
	Schema *mySchema;

	public:
	void Run (Pipe &inPipe, FILE *outFile, Schema &mySchema) ;
	void WaitUntilDone () ;
	void Use_n_Pages (int n) ;
};
#endif
