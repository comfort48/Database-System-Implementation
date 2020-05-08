#include "BigQ.h"
#include <vector>
#include <queue>
#include <algorithm>

/*worker function called by pthread which does the sorting*/
void* worker( void *args);


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
	/*No longer needed*/
	// intialize arguments to constructors
	// this->inputPipe = &in;
	// this->outputPipe = &out;
	// this->sortedOrder = &sortorder;
	// this->runLength = &runlen;

	/*storing the BigQ info differential file*/
	BigQInfo *diffBigQ = new BigQInfo();
	diffBigQ->input = &in;
	diffBigQ->output = &out;
	diffBigQ->sortedOrder = &sortorder;
	diffBigQ->runlen = runlen;

	// creating new File to store runs
	// diffBigQ->runsFile = new File();
	// diffBigQ->runsFile->Open(0, "runs.bin");
	// diffBigQ->runsFile->Close();
	// diffBigQ->runsFile->Open(1, "runs.bin");

	pthread_create(&workerthread, NULL, &worker, (void *)diffBigQ);
	
	// pthread_join( workerthread , NULL );

	// finally shut down the out pipe
	// out.ShutDown();
}

/** Function is no longer needed **/
/* start function from the new worker thread*/ 
// void *BigQ::invoke_tpmmsAlgo(void *args)
// {
// 	// type casting the void arguments and calling the worker function
// 	((BigQ*)args)->worker();
// }

/* worker function that does TPMMS Sorting */
void* worker( void *args)
{
	BigQInfo *mybigqInfo = new BigQInfo();
	mybigqInfo = (BigQInfo*)args;

	char filename[50];

	timespec time;
	clock_gettime(CLOCK_REALTIME, &time);
	int timestamp = time.tv_sec * 1000000000 + time.tv_nsec;
	if (timestamp < 0) timestamp *= -1;

	// Initialize the file that will contain all run information
	sprintf(filename, "bigq__%d.bin", timestamp);

	mybigqInfo->runsFile = new File();
	mybigqInfo->runsFile->Open(0, filename);
	mybigqInfo->runsFile->Close();
	mybigqInfo->runsFile->Open(1, filename);


	int runlen = mybigqInfo->runlen;
	// count number of pages in each Run
	int pagesInRun = 0;
	// Pages is used to full records in a Run
	Page *buffer = new Page();
	// Record memory to get a record from input pipe
	Record *currRecord = new Record();
	vector<int> runPointers;
	// Initial pointer to first Run is zero
	runPointers.push_back(0);
	// vector created to store records of a Run
	vector<Record *> curr_run_vector;
	// Count num of runs
	int num_of_runs = 0;
	// temporary Record used to push records to Current Run vector
	Record *tempRec = new Record();

	// remove records from Input Pipe and create the Sorted Runs
	while (mybigqInfo->input->Remove(currRecord))
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
			mybigqInfo->sortRun(curr_run_vector);
			num_of_runs++;
			// write the sorted Run to the File
			int runhead = mybigqInfo->addRuntoFile(curr_run_vector);
			// adding the head of the current run to list
			runPointers.push_back(runhead);
			curr_run_vector.clear();
			buffer->Append(currRecord);
		}
	}

	// sorting and adding the last run to File
	num_of_runs++;
	while (buffer->GetFirst(tempRec))
	{
		// Schema myschema( "catalog" , "supplier");
		// tempRec->Print(&myschema);

		curr_run_vector.push_back(tempRec);
		tempRec = new Record();
	}
	mybigqInfo->sortRun(curr_run_vector);
	int runhead = mybigqInfo->addRuntoFile(curr_run_vector);
	runPointers.push_back(runhead);
	curr_run_vector.clear();
	mybigqInfo->runsFile->Close();

	/******** Merge Runs Operation  *********/

	// runs File to read the sorted Runs
	mybigqInfo->runsFile = new File();
	mybigqInfo->runsFile->Open(1, filename);
	typedef priority_queue<PriorityQueue_Record *, std::vector<PriorityQueue_Record *>, PriorityQueue_Comparator> pq_merger_type;
	pq_merger_type pq_merger(mybigqInfo->sortedOrder);
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
		mybigqInfo->runsFile->GetPage(runBuffers[i], runPointers[i]);
		runBuffers[i]->GetFirst(currRecord);
		pq_record->currRecord = currRecord;
		pq_record->pageNum = runPointers[i];
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

		mybigqInfo->output->Insert(pq_record->currRecord);
		pq_merger.pop();

		Record *newRecord = new Record();
		if (runBuffers[bufferNum]->GetFirst(newRecord) == 0)
		{

			pageNum = pageNum + 1;
			// moving to next page in the current run of the min record in priority queue
			if (pageNum < (mybigqInfo->runsFile->GetLength() - 1) && (pageNum < runPointers[bufferNum + 1]))
			{
				runBuffers[bufferNum]->EmptyItOut();
				mybigqInfo->runsFile->GetPage(runBuffers[bufferNum], pageNum);

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

	mybigqInfo->runsFile->Close();
	mybigqInfo->output->ShutDown();
	remove(filename);  // remove the bins file no longer needed 
	pthread_exit(NULL);

}

/* writes the sorted vector to File and the returns the current size of file */
int BigQInfo::addRuntoFile(vector<Record *> &vector)
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
void BigQInfo::sortRun(vector<Record *> &vector)
{
	sort(vector.begin(), vector.end(), RecordComparator(sortedOrder));
}

BigQ::~BigQ()
{

}
