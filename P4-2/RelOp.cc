#include "RelOp.h"
#include <iostream>

void *selectFileWorker(void *args)
{

	workerArgs_t *args_t = new workerArgs_t();
	args_t = (workerArgs_t *)args;
	// moving to the first of the file
	args_t->dbFile->MoveFirst();
	// retrieving records and pushing them into the output pipe
	Record tempRecord;
	while (args_t->dbFile->GetNext(tempRecord, *(args_t->cnf), *(args_t->recordliteral)))
	{
		args_t->outputPipe->Insert(&tempRecord);
	}

	args_t->outputPipe->ShutDown();
}

void SelectFile::Run(DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal)
{
	// storing the arguments in struct for thread invocation
	workerArgs_t *args_t = new workerArgs_t();
	args_t->dbFile = &inFile;
	args_t->outputPipe = &outPipe;
	args_t->cnf = &selOp;
	args_t->recordliteral = &literal;

	pthread_create(&thread, NULL, selectFileWorker, (void *)args_t);
}

void SelectFile::WaitUntilDone()
{
	pthread_join(thread, NULL);
}

void SelectFile::Use_n_Pages(int runlen)
{
	this->runLength = runlen;
}

void *SelectPipeWorker(void *args)
{

	workerArgs_t *args_t = new workerArgs_t();
	args_t = (workerArgs_t *)args;

	ComparisonEngine cmpEngine;
	Record tempRec;
	// removing records from the pipe and comparing with the CNF 
	while (args_t->inputPipe->Remove(&tempRec))
	{
		if (cmpEngine.Compare(&tempRec, args_t->recordliteral, args_t->cnf))
			args_t->outputPipe->Insert(&tempRec);
	}

	args_t->outputPipe->ShutDown();
}

void SelectPipe::Run(Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal)
{
	// storing the arguments in struct for thread invocation
	workerArgs_t *args_t = new workerArgs_t();
	args_t->inputPipe = &inPipe;
	args_t->outputPipe = &outPipe;
	args_t->cnf = &selOp;
	args_t->recordliteral = &literal;

	pthread_create(&thread, NULL, SelectPipeWorker, (void *)args_t);
}

void SelectPipe::WaitUntilDone()
{
	pthread_join(thread, NULL);
}

void SelectPipe::Use_n_Pages(int runlen)
{
	this->runLength = runlen;
}

void *ProjectOperation(void *args)
{
	workerArgs_t *args_t = new workerArgs_t();
	args_t = (workerArgs_t *)args;
	ComparisonEngine cmp;
	Record rec;
	// retrieve records from the pipe and push records to ouput pipe with required projection
	while (args_t->inputPipe->Remove(&rec))
	{
		//function which takes each records and retrieves the attributes need for projection 
		rec.Project(args_t->keepMe, args_t->numAttsOutput, args_t->numAttsInput);
		args_t->outputPipe->Insert(&rec);
	}

	args_t->outputPipe->ShutDown();
}

void Project::Run(Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput)
{
	// storing the arguments for the thread invocation 
	workerArgs_t *args_t = new workerArgs_t();
	args_t->inputPipe = &inPipe;
	args_t->outputPipe = &outPipe;
	args_t->keepMe = keepMe;
	args_t->numAttsInput = numAttsInput;
	args_t->numAttsOutput = numAttsOutput;
	pthread_create(&thread, NULL, ProjectOperation, (void *)args_t);
}

void Project::WaitUntilDone()
{
	pthread_join(thread, NULL);
}

void Project::Use_n_Pages(int n)
{
	runLength = n;
}

void *SumOperation(void *args)
{

	workerArgs_t *args_t = new workerArgs_t();
	args_t = (workerArgs_t *)args;

	Record tempRecord;
	ostringstream result;
	string resultSum;
	Record resultRecord;

	// if the sum of the function is Integer type
	if (args_t->computeMe->resultType() == Int)
	{
		int integerSum = 0;
		// removing records from the pipe and sum cumulative
		while (args_t->inputPipe->Remove(&tempRecord))
			integerSum += args_t->computeMe->Apply<int>(tempRecord);
		result << integerSum;
		resultSum = result.str();
		resultSum.append("|");
		Attribute IA = {"int", Int};
		Schema out_sch("out_sch", 1, &IA);
		resultRecord.ComposeRecord(&out_sch, resultSum.c_str());
	}
	else
	{
	// sum of the function is Double type 
		double doubleSum = 0.0;
		// removing records from the pipe and sum cumulative
		while (args_t->inputPipe->Remove(&tempRecord))
			doubleSum += args_t->computeMe->Apply<double>(tempRecord);
		result << doubleSum;
		resultSum = result.str();
		resultSum.append("|");
		Attribute DA = {"double", Double};
		Schema out_sch("out_sch", 1, &DA);
		resultRecord.ComposeRecord(&out_sch, resultSum.c_str());
	}
	//pushing the single record which is the sum into output pipe
	args_t->outputPipe->Insert(&resultRecord);
	args_t->outputPipe->ShutDown();
}

void Sum::Run(Pipe &inPipe, Pipe &outPipe, Function &computeMe)
{
	// storing arguements for thread invocation 
	workerArgs_t *args_t = new workerArgs_t();
	args_t->inputPipe = &inPipe;
	args_t->outputPipe = &outPipe;
	args_t->computeMe = &computeMe;

	pthread_create(&thread, NULL, SumOperation, (void *)args_t);
}

void Sum::WaitUntilDone()
{
	pthread_join(thread, NULL);
}

void Sum::Use_n_Pages(int n)
{
	runLength = n;
}

void *DuplicateRemoveOperation(void *args)
{
	workerArgs_t *args_t = new workerArgs_t();
	args_t = (workerArgs_t *)args;

	// create an ordermaker based on the attributed needed
	OrderMaker sortOrder(args_t->mySchema);
	Pipe *sortedPipe = new Pipe(1000);
	// sort all the records based on the created sorted order
	BigQ BigQ(*args_t->inputPipe, *sortedPipe, sortOrder, 100);
	Record currRecord, nextRecord;
	ComparisonEngine cmp;

	if (sortedPipe->Remove(&currRecord))
	{
		while (sortedPipe->Remove(&nextRecord))
		{
		// compare each consecutive records and add the single record to output pipe
			if (cmp.Compare(&currRecord, &nextRecord, &sortOrder))
			{
				args_t->outputPipe->Insert(&currRecord);
				currRecord.Consume(&nextRecord);
			}
		}
		args_t->outputPipe->Insert(&currRecord);
	}
	args_t->outputPipe->ShutDown();
}

void DuplicateRemoval::Run(Pipe &inPipe, Pipe &outPipe, Schema &mySchema)
{
	// storing the arguments in the struct for thread invocation
	workerArgs_t *args_t = new workerArgs_t();
	args_t->inputPipe = &inPipe;
	args_t->outputPipe = &outPipe;
	args_t->mySchema = &mySchema;
	pthread_create(&thread, NULL, DuplicateRemoveOperation, (void *)args_t);
}

void DuplicateRemoval::WaitUntilDone()
{
	pthread_join(thread, NULL);
}

void DuplicateRemoval::Use_n_Pages(int n)
{
	this->runLength = n;
}

void *WriteOutOperation(void *args)
{
	workerArgs_t *args_t = new workerArgs_t();
	args_t = (workerArgs_t *)args;
	Record currRecord;
	// removing records from the pipe and adding them to file 
	while (args_t->inputPipe->Remove(&currRecord))
	{
		currRecord.Write(args_t->writeOutFile, args_t->mySchema);
	}
}

void WriteOut::Run(Pipe &inPipe, FILE *outFile, Schema &mySchema)
{
	// storing the arguments into structure for thread invocation
	workerArgs_t *args_t = new workerArgs_t();
	args_t->inputPipe = &inPipe;
	args_t->writeOutFile = outFile;
	args_t->mySchema = &mySchema;
	pthread_create(&thread, NULL, WriteOutOperation, (void *)args_t);
}

void WriteOut::WaitUntilDone()
{
	pthread_join(thread, NULL);
}
void WriteOut::Use_n_Pages(int n)
{
	this->runLength = n;
}

void *Join::JoinOperation(void *args)
{
	Join *joinObj = (Join *)(args);

	// creating two ordermakers for left and right tables 
	OrderMaker *leftOrderMaker = new OrderMaker();
	OrderMaker *rightOrderMaker = new OrderMaker();
	// an order comaprison is build on left and right ordermaker using CNF input
	if (joinObj->inputCNF->GetSortOrders(*leftOrderMaker, *rightOrderMaker) == 0)
	{

		// case block nested loops
		
		DBFile db;
		// create file to store the left records
		char *path = "temp.bin";
		db.Create(path, heap, NULL);

		Record *rectoFile = new Record();
		//pushing the records from left table to file
		while (joinObj->inputPipe_L->Remove(rectoFile))
		{
			db.Add(*rectoFile);
		}
		Record recFromPipe;
		Record recFromFile;
		ComparisonEngine comp;
		// moving records from right table and comparing with left table and merging if needed
		while (joinObj->inputPipe_R->Remove(&recFromPipe))
		{
			while (db.GetNext(recFromFile))
			{
				// compare the records from file and right pipe and merge if
				if (!comp.Compare(&recFromPipe, &recFromFile, joinObj->inputliteral, joinObj->inputCNF))
				{
					joinObj->mergeLeftRightrecords(&recFromPipe, &recFromFile);
				}
			}

			db.MoveFirst();
		}
		remove(path);
	}
	else
	{
		// case for sorted merge join

		// two pipe are used for sorting the left and right tables using ordermakers
		Pipe *sortedLeftPipe = new Pipe(1000);
		Pipe *sortedRightPipe = new Pipe(1000);

		bool isLeftPipeEmpty = false;
		bool isRightPipeEmpty = false;

		vector<Record *> leftvector;
		vector<Record *> rightvector;
		ComparisonEngine cmpEngine;

		// left table is sorted based on the left Ordermaker
		BigQ LeftBigQ(*joinObj->inputPipe_L, *sortedLeftPipe, *leftOrderMaker, joinObj->runLength);

		// right table is sorted based on the right Ordermaker
		BigQ RightBigQ(*joinObj->inputPipe_R, *sortedRightPipe, *rightOrderMaker, joinObj->runLength);

		Record *leftRecord = new Record, *rightRecord = new Record;
		// pick the first records from the sorted pipes
		isLeftPipeEmpty = (sortedLeftPipe->Remove(leftRecord) == 0);
		isRightPipeEmpty = (sortedRightPipe->Remove(rightRecord) == 0);

		// a loop for retrieving the records from the left and right sorted pipes 
		while (!isLeftPipeEmpty && !isRightPipeEmpty)
		{
			// compare each record from left pipe and right pipe using their ordermakers
			int res = cmpEngine.Compare(leftRecord, leftOrderMaker, rightRecord, rightOrderMaker);
			if (res < 0)
			{
				// case left less than right so popping next record from left
				isLeftPipeEmpty = (0 == sortedLeftPipe->Remove(leftRecord));
			}
			else if (res > 0)
			{
				// case right less than left so popping next record from right
				isRightPipeEmpty = (0 == sortedRightPipe->Remove(rightRecord));
			}
			else
			{
				// case where there is match and record is used

				//getting next records the pipes and storing them in vectors which match the condition
				isLeftPipeEmpty = joinObj->checkConsecutiveRecords(*sortedLeftPipe, leftRecord, leftvector, leftOrderMaker);
				isRightPipeEmpty = joinObj->checkConsecutiveRecords(*sortedRightPipe, rightRecord, rightvector, rightOrderMaker);

				// now from the both left and right vectors use records to merge
				for (int i = 0; i < leftvector.size(); i++)
				{
					for (int j = 0; j < rightvector.size(); j++)
					{
						// now left and right record is merged 
						joinObj->mergeLeftRightrecords(leftvector[i], rightvector[i]);
					}
				}
			}
		}

		Record t;
		// just popping out any left over records in any one of the pipes 
		while (sortedLeftPipe->Remove(&t));
		while (sortedRightPipe->Remove(&t));

	}

	joinObj->outputPipe->ShutDown();
	pthread_exit(NULL);
}

void Join::mergeLeftRightrecords(Record *left, Record *right)
{
	Record *recordPipe = new Record();
	// storing the left and right attributes of the records
	int numAttsLeft = left->GetNumAtts();
	int numAttsRight = right->GetNumAtts();
	int numAttsTotal = numAttsLeft + numAttsRight;
	// create new array to store values need to store
	int *attsToKeep = (int *)alloca(sizeof(int) * numAttsTotal);
	int index = 0;

	for (int i = 0; i < numAttsLeft; i++)
	{
		attsToKeep[index] = i;
		index++;
	}

	for (int i = 0; i < numAttsRight; i++)
	{
		attsToKeep[index] = i;
		index++;
	}
	// merges the two records into one 
	recordPipe->MergeRecords(left, right, numAttsLeft, numAttsRight, attsToKeep, numAttsTotal, numAttsLeft);
	// Insert into output pipe the merged record
	outputPipe->Insert(recordPipe);
}

bool Join::checkConsecutiveRecords(Pipe &pipe, Record *rec, vector<Record *> &vecOfRecs, OrderMaker *sortOrder)
{
	vecOfRecs.clear();

	Record *curr = new Record();
	ComparisonEngine cmpEngine;
	// send the first record into the vector
	curr->Consume(rec);
	vecOfRecs.push_back(curr);

	bool isEmpty = (pipe.Remove(rec) == 0);
	// storing the bool to check if the pipe is empty and consuctive retrive records with matching record
	while (!isEmpty && cmpEngine.Compare(curr, rec, sortOrder) == 0)
	{
		vecOfRecs.push_back(rec);
		curr->Consume(rec);
		isEmpty = (pipe.Remove(rec) == 0);
	}

	return isEmpty;
}

void Join::Run(Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal)
{
	// storing the arguments for invoking the thread
	this->inputPipe_L = &inPipeL;
	this->inputPipe_R = &inPipeR;
	this->outputPipe = &outPipe;
	this->inputCNF = &selOp;
	this->inputliteral = &literal;
	pthread_create(&thread, NULL, JoinOperation, (void *)this);
}
void Join::WaitUntilDone()
{
	pthread_join(thread, NULL);
}
void Join::Use_n_Pages(int n)
{
	this->runLength = n;
}


void* GroupBy::GroupByOperation(void *args){
	
	GroupBy *grpByObj = (GroupBy *)(args);

	Type type;
	Record *prevRecord = NULL;
	Record *currRecord = new Record();
	
	int intResult = 0;
	double doubleResult = 0.0;

	// taking Pipe and sorting the pipe based on the input Order
	Pipe sortedPipe(1000);
	BigQ sort( *grpByObj->inputPipe, sortedPipe , *grpByObj->inputOrder, grpByObj->runLength );

	ComparisonEngine cmpEngine;
	// looping through all the records from the sorted pipe
	while( sortedPipe.Remove(currRecord) ){

		if( prevRecord == NULL ){
			// initial case where the prevRecord is null
			prevRecord = new Record();
			prevRecord->Copy(currRecord);
			type = grpByObj->aggregateSum( currRecord, intResult, doubleResult );
		}
		else{
			// consequent cases where both next and prevRecordious records exist

			// the current and previous record match with input order so aggRecord the sum
			if( cmpEngine.Compare( currRecord, prevRecord, grpByObj->inputOrder ) == 0 ){
				
				type = grpByObj->aggregateSum( currRecord, intResult, doubleResult); 

			}
			else{
			// else case where aggRecord sum is reset to zero and new group is created 
				
				grpByObj->createRecordwithGrpSum( prevRecord, type, intResult, doubleResult );
				prevRecord->Copy(currRecord);
				doubleResult = 0.0;
				intResult = 0;
				// aggRecord the sum over the new group
				type = grpByObj->aggregateSum( currRecord, intResult, doubleResult ); 

			}

		}

	}
	// create the last group record
	grpByObj->createRecordwithGrpSum( prevRecord, type, intResult, doubleResult );

	delete prevRecord;
	delete currRecord;
	grpByObj->outputPipe->ShutDown();
	
	pthread_exit(NULL);

}

void GroupBy::createRecordwithGrpSum(Record * rec, Type type, int isum, double dsum) {
	Record *aggRecord = new Record;
	Record *recordPipe = new Record;

	// create new attribute for sum
	Attribute *atts = new Attribute[1];
	std::stringstream s;
	if (type == Int) {
		s << isum;
	}
	else {
		s << dsum;
	}

	s << "|";
	atts[0].name = "SUM";
	atts[0].myType = type;
	Schema sumSchema("grpSchema", 1, atts);
	// using the compose record to build the records with the schema and string
	aggRecord->ComposeRecord(&sumSchema, s.str().c_str());

	// build the remaining attributes for this new record
	int numAttsToKeep = 1 + inputOrder->numAtts;
	int *attsToKeep = new int[numAttsToKeep];
	attsToKeep[0] = 0;

	for (int i = 1; i < numAttsToKeep; i++) {
		attsToKeep[i] = inputOrder->whichAtts[i - 1];
	}
	// merge this record with incoming record, push new record attribute to output pipe
	recordPipe->MergeRecords(aggRecord, rec, 1, inputOrder->numAtts, attsToKeep, numAttsToKeep, 1);
	outputPipe->Insert(recordPipe);

	delete recordPipe;
	delete aggRecord;
	delete[] atts;
	delete[] attsToKeep;

}

Type GroupBy::aggregateSum(Record * rec, int & isum, double & dsum) {
	
	int iIncrement = 0;
	double dIncrement = 0.0;
	Type type;
	type = inputFunc-> Apply(*rec, iIncrement, dIncrement);
	isum += iIncrement;
	dsum += dIncrement;
	return type;
}

void GroupBy::Run(Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe)
{
	// storing the arguments for invoking the thread
	this->inputPipe = &inPipe;
	this->outputPipe = &outPipe;
	this->inputOrder = &groupAtts;
	this->inputFunc = &computeMe;
	
	pthread_create(&thread, NULL, GroupByOperation, (void *)this);
}

void GroupBy::WaitUntilDone()
{
	pthread_join(thread, NULL);
}

void GroupBy::Use_n_Pages(int runlen)
{
	this->runLength = runlen;
}