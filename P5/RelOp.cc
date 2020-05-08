#include "RelOp.h"
#include <fstream>
#include <iostream>
#include <vector>
#include "BigQ.h"

static int clear_pipe (Pipe &in_pipe, Schema *schema, Function &func, bool print) ;
static int clear_pipe (Pipe &in_pipe, Schema *schema, bool print) ;


#define CLEANUPVECTOR(v)                                                     \
	({                                                                       \
		for (vector<Record *>::iterator it = v.begin(); it != v.end(); it++) \
		{                                                                    \
			if (!*it)                                                        \
			{                                                                \
				delete *it;                                                  \
			}                                                                \
		}                                                                    \
		v.clear();                                                           \
	})

void *SelectFile::selectFileWorker(void *arg)
{
	SelectFile *sf = (SelectFile *)arg;

	Record *tmpRecord = new Record;
	// moving to the first of the file
	sf->dbFile->MoveFirst();
	// retrieving records and pushing them into the output pipe
	while (sf->dbFile->GetNext(*tmpRecord, *(sf->cnf), *(sf->recordliteral)))
	{
		sf->outputPipe->Insert(tmpRecord);
	}
	delete tmpRecord;
	sf->dbFile->Close();
	sf->outputPipe->ShutDown();

	return NULL;
}

void SelectFile::Run(DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal)
{
	// storing the arguments for thread invocation
	this->dbFile = &inFile;
	this->outputPipe = &outPipe;
	this->cnf = &selOp;
	this->recordliteral = &literal;
	pthread_create(&thread, NULL, selectFileWorker, this);
}

void SelectFile::WaitUntilDone()
{
	pthread_join(thread, NULL);
}

void SelectFile::Use_n_Pages(int runlen)
{
	this->nPages = runlen;
}

void *SelectPipe::selectPipeWorker(void *arg)
{
	SelectPipe *sp = (SelectPipe *)arg;
	Record *tmpRecord = new Record();
	while (sp->inputPipe->Remove(tmpRecord))
	{
		ComparisonEngine cmp;
		if (cmp.Compare(tmpRecord, sp->recordliteral, sp->cnf))
		{
			sp->outputPipe->Insert(tmpRecord);
		}
	}
	delete tmpRecord;
	sp->outputPipe->ShutDown();
}

void SelectPipe::Run(Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal)
{
	// storing the arguments for thread invocation
	this->inputPipe = &inPipe;
	this->outputPipe = &outPipe;
	this->cnf = &selOp;
	this->recordliteral = &literal;
	pthread_create(&thread, NULL, selectPipeWorker, this);
}

void SelectPipe::WaitUntilDone()
{
	pthread_join(thread, NULL);
}

void SelectPipe::Use_n_Pages(int runlen)
{
	this->nPages = runlen;
}

void *Project::projectWorker(void *arg)
{
	Project *pj = (Project *)arg;
	Record *tmpRcd = new Record;
	// retrieve records from the pipe and push records to ouput pipe with required projection
	while (pj->inputPipe->Remove(tmpRcd))
	{
		//function which takes each records and retrieves the attributes need for projection
		tmpRcd->Project(pj->keepMe, pj->numAttsOutput, pj->numAttsInput);
		pj->outputPipe->Insert(tmpRcd);
	}
	pj->outputPipe->ShutDown();
	delete tmpRcd;
}

void Project::Run(Pipe &inPipe, Pipe &outPipe, int *keepMe,
				  int numAttsInput, int numAttsOutput)
{
	// storing the arguments for the thread invocation
	this->inputPipe = &inPipe;
	this->outputPipe = &outPipe;
	this->keepMe = keepMe;
	this->numAttsInput = numAttsInput;
	this->numAttsOutput = numAttsOutput;
	pthread_create(&thread, NULL, projectWorker, this);
}

void Project::WaitUntilDone()
{
	pthread_join(thread, NULL);
}

void Project::Use_n_Pages(int n)
{
	this->nPages = n;
}

void *Join::join(void *arg)
{
	Join *mj = (Join *)arg;
	mj->DoJoin();
}

void Join::DoJoin()
{

	OrderMaker orderL;
	OrderMaker orderR;
	this->inputCNF->GetSortOrders(orderL, orderR);

	if (orderL.numAtts && orderR.numAtts && orderL.numAtts == orderR.numAtts)
	{
		// case for sorted merge join
		// two pipe are used for sorting the left and right tables using ordermakers

		Pipe pipeL(1000), pipeR(1000);
		BigQ *bigQL = new BigQ(*(this->inputPipe_L), pipeL, orderL, RUNLEN);
		BigQ *bigQR = new BigQ(*(this->inputPipe_R), pipeR, orderR, RUNLEN);

		vector<Record *> vectorL;
		vector<Record *> vectorR;
		Record *rcdL = new Record();
		Record *rcdR = new Record();
		ComparisonEngine cmp;

		if (pipeL.Remove(rcdL) && pipeR.Remove(rcdR))
		{
			int leftAttr = ((int *)rcdL->bits)[1] / sizeof(int) - 1;
			int rightAttr = ((int *)rcdR->bits)[1] / sizeof(int) - 1;
			int totalAttr = leftAttr + rightAttr;
			int attrToKeep[totalAttr];
			for (int i = 0; i < leftAttr; i++)
				attrToKeep[i] = i;
			for (int i = 0; i < rightAttr; i++)
				attrToKeep[i + leftAttr] = i;
			int joinNum;

			bool leftOK = true, rightOK = true; 
			//means that record from left and right are both ok
			int num = 0;
			while (leftOK && rightOK)
			{
				leftOK = false;
				rightOK = false;
				int cmpRst = cmp.Compare(rcdL, &orderL, rcdR, &orderR);
				switch (cmpRst)
				{
				case 0: 
				{
					// left record is equal to right
					num++;
					Record *rcd1 = new Record();
					rcd1->Consume(rcdL);
					Record *rcd2 = new Record();
					rcd2->Consume(rcdR);
					vectorL.push_back(rcd1);
					vectorR.push_back(rcd2);
					//get rcds from pipeL that equal to rcdL
					while (pipeL.Remove(rcdL))
					{
						if (0 == cmp.Compare(rcdL, rcd1, &orderL))
						{	// equal
							Record *cLMe = new Record();
							cLMe->Consume(rcdL);
							vectorL.push_back(cLMe);
						}
						else
						{
							leftOK = true;
							break;
						}
					}
					//get rcds from PipeR that equal to rcdR
					while (pipeR.Remove(rcdR))
					{
						if (0 == cmp.Compare(rcdR, rcd2, &orderR))
						{	
							Record *cRMe = new Record();
							cRMe->Consume(rcdR);
							vectorR.push_back(cRMe);
						}
						else
						{
							rightOK = true;
							break;
						}
					}
					//now we have the two vectors that can do cross product
					Record *lr = new Record, *rr = new Record, *jr = new Record;
					for (vector<Record *>::iterator itL = vectorL.begin(); itL != vectorL.end(); itL++)
					{
						lr->Consume(*itL);
						for (vector<Record *>::iterator itR = vectorR.begin(); itR != vectorR.end(); itR++)
						{
							//join both records and output
							if (1 == cmp.Compare(lr, *itR, this->inputliteral, this->inputCNF))
							{
								joinNum++;
								rr->Copy(*itR);
								jr->MergeRecords(lr, rr, leftAttr, rightAttr, attrToKeep, leftAttr + rightAttr, leftAttr);
								this->outputPipe->Insert(jr);
							}
						}
					}
					CLEANUPVECTOR(vectorL);
					CLEANUPVECTOR(vectorR);

					break;
				}
				case 1: 
					// left record > right record
					leftOK = true;
					if (pipeR.Remove(rcdR))
						rightOK = true;
					break;
				case -1: 
					// left record < right record
					rightOK = true;
					if (pipeL.Remove(rcdL))
						leftOK = true;
					break;
				}
			}
		}
	}
	else
	{ 
		// case block nested loops
		cout << "block nested loops join" << endl;
		//assume the size of left relation is less than right relation
		int n_pages = 10;
		// take n_pages-1 pages from right, and 1 page from left
		Record *rcdL = new Record;
		Record *rcdR = new Record;
		Page pageR;
		DBFile dbFileL;
		fType ft = heap;
		dbFileL.Create((char *)"tmpL", ft, NULL);
		dbFileL.MoveFirst();

		int leftAttr, rightAttr, totalAttr, *attrToKeep;

		if (this->inputPipe_L->Remove(rcdL) && this->inputPipe_R->Remove(rcdR))
		{
			//merge the attributed from left and right record
			leftAttr = ((int *)rcdL->bits)[1] / sizeof(int) - 1;
			rightAttr = ((int *)rcdR->bits)[1] / sizeof(int) - 1;
			totalAttr = leftAttr + rightAttr;
			attrToKeep = new int[totalAttr];
			for (int i = 0; i < leftAttr; i++)
				attrToKeep[i] = i;
			for (int i = 0; i < rightAttr; i++)
				attrToKeep[i + leftAttr] = i;

			//move records from leftpipe to dbfile
			do
			{
				dbFileL.Add(*rcdL);
			} while (this->inputPipe_L->Remove(rcdL));


			vector<Record *> vectorR;
			ComparisonEngine cmp;

			bool rMore = true;
			int joinNum = 0;
			while (rMore)
			{
				Record *first = new Record();
				first->Copy(rcdR);
				pageR.Append(rcdR);
				vectorR.push_back(first);
				int rPages = 0;

				rMore = false;
				while (this->inputPipe_R->Remove(rcdR))
				{
					//getting n-1 pages of records into vectorR
					Record *copyMe = new Record();
					copyMe->Copy(rcdR);
					if (!pageR.Append(rcdR))
					{
						rPages += 1;
						if (rPages >= n_pages - 1)
						{
							rMore = true;
							break;
						}
						else
						{
							pageR.EmptyItOut();
							pageR.Append(rcdR);
							vectorR.push_back(copyMe);
						}
					}
					else
					{
						vectorR.push_back(copyMe);
					}
				}
				
				// now we have the n-1 pages records from Right relation
				dbFileL.MoveFirst(); //we should do this in each iteration
				//iterate all the tuples in Left
				int fileRN = 0;
				while (dbFileL.GetNext(*rcdL))
				{
					for (vector<Record *>::iterator it = vectorR.begin(); it != vectorR.end(); it++)
					{
						if (1 == cmp.Compare(rcdL, *it, this->inputliteral, this->inputCNF))
						{
							//applied to the CNF, then join
							joinNum++;
							Record *jr = new Record();
							Record *rr = new Record();
							rr->Copy(*it);
							jr->MergeRecords(rcdL, rr, leftAttr, rightAttr, attrToKeep, leftAttr + rightAttr, leftAttr);
							this->outputPipe->Insert(jr);
						}
					}
				}
				//clean up the vectorR
				CLEANUPVECTOR(vectorR);
			}
			dbFileL.Close();
		}
	}
	this->outputPipe->ShutDown();
}

void Join::Run(Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal)
{
	this->inputPipe_L = &inPipeL;
	this->inputPipe_R = &inPipeR;
	this->outputPipe = &outPipe;
	this->inputCNF = &selOp;
	this->inputliteral = &literal;
	pthread_create(&thread, NULL, join, this);
}
void Join::WaitUntilDone()
{
	pthread_join(thread, NULL);
}
void Join::Use_n_Pages(int n)
{
	this->nPages = n;
}

void *DuplicateRemoval::duplicateRemoval(void *arg)
{
	DuplicateRemoval *drObj = (DuplicateRemoval *)arg;


	Record *currRec = new Record();
	Pipe *sortedrecs = new Pipe(1000);
	ComparisonEngine cp;
	Record *temp = new Record();
	Record *prevRec = NULL;
		vector<int> indexOfAttributesToKeep;

	int  index = drObj->mySchema->Find("r.r_regionkey");
	
	drObj->mySchema->Print();
	cout << "jjjj" << endl;
	// Init the bigQ to sort the records
	OrderMaker inputOrder(drObj->mySchema , index);
	BigQ sortPipe( *drObj->inputPipe, *sortedrecs, inputOrder, drObj->nPages);
	inputOrder.Print();
	
	Record *nextRecord = new Record();
	cout << "fff" << endl;

	if (sortedrecs->Remove(currRec))
	{

		while (sortedrecs->Remove(nextRecord))
		{
			cout << "fff" << endl;
		// compare each consecutive records and add the single record to output pipe
			if (cp.Compare(currRec, nextRecord, &inputOrder))
			{
				drObj->outputPipe->Insert(currRec);
				currRec->Consume(nextRecord);
			}
		}
		drObj->outputPipe->Insert(currRec);
	}
	cout << "hello " << endl;
	delete temp;
	delete prevRec;
	delete currRec;

	// shutdown the pipe
	sortedrecs->ShutDown();
	drObj->outputPipe->ShutDown();
	//clear_pipe( *drObj->outputPipe, &sumSchema , true );

	// return NULL;
}

void DuplicateRemoval::Run(Pipe &inPipe, Pipe &outPipe, Schema &mySchema)
{
	this->inputPipe = &inPipe;
	this->outputPipe = &outPipe;
	this->mySchema = &mySchema;
		cout << "uuuuuu" << endl;

	pthread_create(&thread, NULL, duplicateRemoval, this);
}
void DuplicateRemoval::WaitUntilDone()
{
	pthread_join(thread, NULL);
}
void DuplicateRemoval::Use_n_Pages(int n)
{
	this->nPages = n;
}

void *Sum::SumWorker(void *arg)
{
	Sum *sum = (Sum *)arg;

	Record *tmpRcd = new Record;

	double result = 0.0;
	Type type;
	while (sum->inputPipe->Remove(tmpRcd))
	{
		int ir;
		double dr;
		type = sum->computeMe->Apply(*tmpRcd, ir, dr);
		result += (ir + dr);
	}
	//compose a record and put it into the outPipe;
	Attribute *attr = new Attribute;
	attr->name = (char *)"sum";
	attr->myType = Double;

	Schema *schema = new Schema((char *)"dummy", 1, attr);

	ostringstream ss;
	ss << result;
	ss << "|";

	Record *rcd = new Record();
	rcd->ComposeRecord(schema, ss.str().c_str());

	sum->outputPipe->Insert(rcd);

	sum->outputPipe->ShutDown();

	return NULL;
}

void Sum::Run(Pipe &inPipe, Pipe &outPipe, Function &computeMe)
{
	this->inputPipe = &inPipe;
	this->outputPipe = &outPipe;
	this->computeMe = &computeMe;
	pthread_create(&thread, NULL, SumWorker, this);
}
void Sum::WaitUntilDone()
{
	pthread_join(thread, NULL);
}
void Sum::Use_n_Pages(int n)
{
	this->nPages = n;
}

void *GroupBy::groupBy(void *arg)
{
	GroupBy *grpByObj = (GroupBy *)arg;
	// gb->DoGroupBy();
	// return NULL;

	Type type;
	Record *prevRecord = NULL;
	Record *currRecord = new Record();

	int intResult = 0;
	double doubleResult = 0.0;

	// taking Pipe and sorting the pipe based on the input Order
	Pipe sortedPipe(1000);
	BigQ sort(*grpByObj->inputPipe, sortedPipe, *grpByObj->inputOrder, grpByObj->nPages);

	 //cout << "ooooooooooo " << endl;


	ComparisonEngine cmpEngine;
	// looping through all the records from the sorted pipe
	while (sortedPipe.Remove(currRecord))
	{
		if (prevRecord == NULL)
		{
			// initial case where the prevRecord is null
			prevRecord = new Record();
			// type = grpByObj->aggregateSum(currRecord, intResult, doubleResult);

			prevRecord->Copy(currRecord);
			type = grpByObj->aggregateSum(currRecord, intResult, doubleResult);
			 	//cout << "lllllllllllll " << endl;
			// prevRecord->Consume(currRecord);
			// currRecord = new Record();
		}
		else
		{
			// consequent cases where both next and prevRecordious records exist

			// the current and previous record match with input order so aggRecord the sum
			if (cmpEngine.Compare(currRecord, prevRecord, grpByObj->inputOrder) == 0)
			{

				type = grpByObj->aggregateSum(currRecord, intResult, doubleResult);
			}
			else
			{
				// else case where aggRecord sum is reset to zero and new group is created

				grpByObj->createRecordwithGrpSum(prevRecord, type, intResult, doubleResult);
				cout<<"YOYOYOYOooooooooooooooooooooooooooooooooooooooooooooo"<<endl;
				// prevRecord->Copy(currRecord);
				prevRecord->Consume(currRecord);
			// currRecord = new Record();
				doubleResult = 0.0;
				intResult = 0;
				// aggRecord the sum over the new group
				type = grpByObj->aggregateSum(currRecord, intResult, doubleResult);
			}
		}
	}
	// create the last group record
	grpByObj->createRecordwithGrpSum(prevRecord, type, intResult, doubleResult);

	Attribute *atts = new Attribute[1];
	atts[0].name = "SUM";
	atts[0].myType = type;
	Schema sumSchema("grpSchema", 1, atts);

	delete prevRecord;
	delete currRecord;
	grpByObj->outputPipe->ShutDown();
	clear_pipe( *grpByObj->outputPipe, &sumSchema , true );
}

void GroupBy::createRecordwithGrpSum(Record *rec, Type type, int isum, double dsum)
{
	Record *aggRecord = new Record;
	Record *recordPipe = new Record;

	// create new attribute for sum
	Attribute *atts = new Attribute[1];
	std::stringstream s;
	if (type == Int)
	{
		s << isum;
	}
	else
	{
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

	for (int i = 1; i < numAttsToKeep; i++)
	{
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

Type GroupBy::aggregateSum(Record *rec, int &isum, double &dsum)
{
	int iIncrement = 0;
	double dIncrement = 0.0;
	Type type;
	type = inputFunc->Apply(*rec, iIncrement, dIncrement);
	isum += iIncrement;
	dsum += dIncrement;
	return type;
}

void GroupBy::Run(Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe)
{
	this->inputPipe = &inPipe;
	this->outputPipe = &outPipe;
	this->inputOrder = &groupAtts;
	this->inputFunc = &computeMe;
	pthread_create(&thread, NULL, groupBy, this);
	//pthread_join(thread, NULL);
}
void GroupBy::WaitUntilDone()
{
	pthread_join(thread, NULL);
}
void GroupBy::Use_n_Pages(int n)
{
	this->nPages = n;
}

void *WriteOut::writeOutWorker(void *arg)
{
	WriteOut *woObj = (WriteOut *)arg;
	Record currRecord;
	// removing records from the pipe and adding them to file
	while (woObj->inputPipe->Remove(&currRecord))
	{
		currRecord.Write(woObj->writeOutFile, woObj->mySchema);
	}
	fclose(woObj->writeOutFile);
}

void WriteOut::Run(Pipe &inPipe, FILE *outFile, Schema &mySchema)
{
	this->inputPipe = &inPipe;
	this->writeOutFile = outFile;
	this->mySchema = &mySchema;
	pthread_create(&thread, NULL, writeOutWorker, this);
}
void WriteOut::WaitUntilDone()
{
	pthread_join(thread, NULL);
}
void WriteOut::Use_n_Pages(int n)
{
	this->nPages = n;
}

int clear_pipe (Pipe &in_pipe, Schema *schema, bool print) {
	Record rec;
	int cnt = 0;
	while (in_pipe.Remove (&rec)) {
		if (print) {
			rec.Print (schema);
		}
		cnt++;
	}
	return cnt;
}

int clear_pipe (Pipe &in_pipe, Schema *schema, Function &func, bool print) {
	Record rec;
	int cnt = 0;
	double sum = 0;
	while (in_pipe.Remove (&rec)) {
		if (print) {
			rec.Print (schema);
		}
		int ival = 0; double dval = 0;
		func.Apply (rec, ival, dval);
		sum += (ival + dval);
		cnt++;
	}
	cout << " Sum: " << sum << endl;
	return cnt;
}
