#include "BigQ.h"
#include <vector>
#include <queue>
#include <algorithm>

/* Struct required for priority queue which encloses the current record
	with it current page and the run Number */
struct PriorityQueue_Record
{
	Record *currRecord;
	int pageNum;
	int bufferNum;
};

/* Comparator for priority queue to maintain the Min Heap Property */
class PriorityQueue_Comparator
{
private:
	OrderMaker *sortOrder;
	ComparisonEngine cmp;

public:
	PriorityQueue_Comparator(OrderMaker *sortOrder)
	{
		this->sortOrder = sortOrder;
	}
	bool operator()(PriorityQueue_Record *r1, PriorityQueue_Record *r2)
	{
		if (cmp.Compare(r1->currRecord, r2->currRecord, sortOrder) > 0)
		{
			return true;
		}
		return false;
	}
};

/* Comparator uses the ComparisonEngine to sort vector of records */
class RecordComparator
{
private:
	OrderMaker *sortOrder;
	ComparisonEngine cmp;

public:
	RecordComparator(OrderMaker *sortOrder)
	{
		this->sortOrder = sortOrder;
	}
	bool operator()(Record *r1, Record *r2)
	{
		if (cmp.Compare(r1, r2, sortOrder) < 0)
			return true;
		return false;
	}
};

/* Constructor for BigQ class */
BigQ ::BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen)
{
	// intialize arguments to constructors
	this->inputPipe = &in;
	this->outputPipe = &out;
	this->sortedOrder = &sortorder;
	this->runLength = &runlen;

	// creating new File to store runs
	this->runsFile = new File();
	this->runsFile->Open(0, "runs.bin");
	this->runsFile->Close();
	this->runsFile->Open(1, "runs.bin");

	pthread_t workerthread;
	pthread_create(&workerthread, NULL, BigQ::invoke_tpmmsAlgo, (void *)this);
	pthread_join(workerthread, NULL);

	// finally shut down the out pipe
	out.ShutDown();
}

/* start function from the new worker thread*/
void *BigQ::invoke_tpmmsAlgo(void *args)
{
	// type casting the void arguments and calling the worker function
	((BigQ *)args)->worker();
}

/* worker function that does TPMMS Sorting */
void BigQ::worker()
{

	int runlen = *runLength;
	// count number of pages in each Run
	int pagesInRun = 0;
	// Pages is used to full records in a Run
	Page *buffer = new Page();
	// Record memory to get a record from input pipe
	Record *currRecord = new Record();
	// Initial pointer to first Run is zero
	runPointers.push_back(0);
	// vector created to store records of a Run
	vector<Record *> curr_run_vector;
	// Count num of runs
	int num_of_runs = 0;
	// temporary Record used to push records to Current Run vector
	Record *tempRec = new Record();

	// remove records from Input Pipe and create the Sorted Runs
	while (inputPipe->Remove(currRecord))
	{

		// pages in Run is less than run Length
		if (pagesInRun < runlen)
		{

			int result = buffer->Append(currRecord);
			if (result == 0)
			{
				pagesInRun++;
				while (buffer->GetFirst(tempRec))
				{
					curr_run_vector.push_back(tempRec);
					tempRec = new Record();
				}
				buffer->EmptyItOut();
				buffer->Append(currRecord);
			}
		}
		else
		{
			// pages in Run is equal to run Length
			pagesInRun = 0;
			// sort a current Run
			this->sortRun(curr_run_vector);
			num_of_runs++;
			// write the sorted Run to the File
			int runhead = this->addRuntoFile(curr_run_vector);
			// adding the head of the current run to list
			this->runPointers.push_back(runhead);
			curr_run_vector.clear();
			buffer->Append(currRecord);
		}
	}
	// sorting and adding the last run to File
	num_of_runs++;
	while (buffer->GetFirst(tempRec))
	{
		curr_run_vector.push_back(tempRec);
		tempRec = new Record();
	}
	this->sortRun(curr_run_vector);
	int runhead = this->addRuntoFile(curr_run_vector);
	this->runPointers.push_back(runhead);
	curr_run_vector.clear();
	this->runsFile->Close();

	/******** Merge Runs Operation  *********/

	// runs File to read the sorted Runs
	this->runsFile = new File();
	runsFile->Open(1, "runs.bin");
	typedef priority_queue<PriorityQueue_Record *, std::vector<PriorityQueue_Record *>, PriorityQueue_Comparator> pq_merger_type;
	pq_merger_type pq_merger(sortedOrder);
	// array of buffers get pages from Runs
	Page *runBuffers[num_of_runs];
	// Priority Queue used to merge the records from the runs
	PriorityQueue_Record *pq_record = new PriorityQueue_Record();
	currRecord = new Record();
	int i = 0;
	// pushing the first record of every run to priority queue
	while (i < num_of_runs)
	{
		runBuffers[i] = new Page();
		runsFile->GetPage(runBuffers[i], this->runPointers[i]);
		runBuffers[i]->GetFirst(currRecord);
		pq_record->currRecord = currRecord;
		pq_record->pageNum = this->runPointers[i];
		pq_record->bufferNum = i;
		pq_merger.push(pq_record);
		pq_record = new PriorityQueue_Record();
		currRecord = new Record();
		i++;
	}

	// retrieving minimum record from priority queue and pushing to output pipe
	while (!pq_merger.empty())
	{

		pq_record = pq_merger.top();
		int pageNum = pq_record->pageNum;
		int bufferNum = pq_record->bufferNum;
		this->outputPipe->Insert(pq_record->currRecord);
		pq_merger.pop();

		Record *newRecord = new Record();
		if (runBuffers[bufferNum]->GetFirst(newRecord) == 0)
		{

			pageNum = pageNum + 1;
			// moving to next page in the current run of the min record in priority queue
			if (pageNum < (runsFile->GetLength() - 1) && (pageNum < this->runPointers[bufferNum + 1]))
			{
				runBuffers[bufferNum]->EmptyItOut();
				runsFile->GetPage(runBuffers[bufferNum], pageNum);

				if (runBuffers[bufferNum]->GetFirst(newRecord) != 0)
				{
					pq_record->currRecord = newRecord;
					pq_record->bufferNum = bufferNum;
					pq_record->pageNum = pageNum;
					pq_merger.push(pq_record);
				}
			}
		}
		else
		{
			pq_record->currRecord = newRecord;
			pq_record->bufferNum = bufferNum;
			pq_record->pageNum = pageNum;
			pq_merger.push(pq_record);
		}
	}

	runsFile->Close();
}

/* writes the sorted vector to File and the returns the current size of file */
int BigQ::addRuntoFile(vector<Record *> &vector)
{

	Page *buffer = new Page();

	for (int i = 0; i < vector.size(); i++)
	{
		if (buffer->Append(vector[i]) == 0)
		{
			if (this->runsFile->GetLength() == 0)
			{
				this->runsFile->AddPage(buffer, 0);
			}
			else
			{
				this->runsFile->AddPage(buffer, this->runsFile->GetLength() - 1);
			}
			buffer->EmptyItOut();
			buffer->Append(vector[i]);
		}
	}
	if (this->runsFile->GetLength() == 0)
	{
		this->runsFile->AddPage(buffer, 0);
	}
	else{
		this->runsFile->AddPage(buffer, this->runsFile->GetLength() - 1);
	}
	// return the size of File which is runHead for the next run 
	return this->runsFile->GetLength() - 1; 
}

/* sort the vector based on the input sorted Order using comparator */
void BigQ::sortRun(vector<Record *> &vector)
{
	sort(vector.begin(), vector.end(), RecordComparator(sortedOrder));
}

BigQ::~BigQ()
{
	this->outputPipe->ShutDown();
}
