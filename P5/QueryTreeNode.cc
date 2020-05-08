#include "QueryTreeNode.h"
#include "DBFile.h"
#include "SortedFile.h"

QueryTreeNode::QueryTreeNode() {
	this->parent = NULL;
	this->left = NULL;
	this->right = NULL;
	this->cnf = new CNF;
	this->literal = new Record;
}
QueryTreeNode::~QueryTreeNode(){
	delete cnf;
	delete literal;
}

QueryTree::QueryTree() {
	this->pipeNum = 0;
	this->dbNum = 0;
	this->output = new char[50];
	FILE *fp = fopen(output_path, "r");
	fscanf(fp, "%s", this->output);
	fclose(fp);
}

QueryTree::~QueryTree() {
}

void QueryTree::PrintInOrder() {
	PrintNode(root);
}

void QueryTree::PrintNode(QueryTreeNode *node) {
	if(node->left)
		PrintNode(node->left);

	if(node->right)
		PrintNode(node->right);

	

	//print node
	switch(node->opType) {
	case SELECTFILE:
    	cout<<" *********** "<<endl;
		cout <<"SELECTFILE Operation"<<endl;
		cout <<"Input Pipe "<<node->lPipeId<<endl;
		cout <<"Output Pipe "<<node->outPipeId<<endl;
		cout <<"Output Schema: " <<endl;
			node->outputSchema->Print();
		cout <<"Select CNF: " <<endl;
		cout <<"\t"; node->cnf->Print();
		cout <<"\n\n";
		break;
	case SELECTPIPE:
		cout <<"*****************"<<endl;
		cout <<"SELECTPIPE Operation"<<endl;
		cout <<"Input Pipe "<<node->lPipeId<<endl;
		cout <<"Output Pipe "<<node->outPipeId<<endl;
		cout <<"Output Schema: " <<endl;
			node->outputSchema->Print();
		cout <<"Select CNF: " <<endl;
		cout <<"\t"; node->cnf->Print();
		cout <<"\n\n";
		break;
	case PROJECT:
		cout <<"*****************"<<endl;
		cout <<"PROJECT Operation"<<endl;
		cout <<"Input Pipe 	"<<node->lPipeId<<endl;
		cout <<"Output Pipe "<<node->outPipeId<<endl;
		cout <<"Output Schema: " <<endl;
		node->outputSchema->Print();
		cout <<"Attributes to keep: "<<endl;
		cout <<"\t";
		for(int i=0;i<node->numAttsOutput;i++) {
			cout <<node->keepMe[i] <<", ";
		}
		cout <<endl;
		cout <<"\n";
		break;
	case JOIN:
		cout <<"*****************"<<endl;
		cout <<"JOIN Operation"<<endl;
		cout <<"Left Input Pipe "<<node->lPipeId<<endl;
		cout <<"Right Input Pipe "<<node->rPipeId<<endl;
		cout <<"Output Pipe "<<node->outPipeId<<endl;
		cout <<"Output Schema: " <<endl;
			node->outputSchema->Print();
		cout <<"Select CNF: " <<endl;
		cout <<"\t"; node->cnf->Print();
		cout <<"\n\n";
		break;
	case SUM:
		cout <<"*****************"<<endl;
		cout <<"SUM Operation"<<endl;
		cout <<"Input Pipe	"<<node->lPipeId<<endl;
		cout <<"Output Pipe "<<node->outPipeId<<endl;
		cout <<"Output Schema: " <<endl;
		node->outputSchema->Print();
		cout <<"Function: " <<endl;
		node->function->Print();
		cout <<endl;
		cout <<"\n";
		break;
	case GROUP_BY:
		cout <<"*****************"<<endl;
		cout <<"GROUP BY Operation"<<endl;
		cout <<"Input Pipe	"<<node->lPipeId<<endl;
		cout <<"Output Pipe "<<node->outPipeId<<endl;
		cout <<"Output Schema " <<endl;
		node->outputSchema->Print();
		cout <<"Group By OrderMaker: " <<endl;
		node->orderMaker->Print();
		cout <<endl;
		cout <<"Group By Function: " <<endl;
		node->function->Print();
		cout <<endl;
		cout <<"\n";
		break;
	case DISTINCT:
		cout <<"*****************"<<endl;
		cout <<"DISTINCT Operation"<<endl;
		cout <<"Input Pipe:	"<<node->lPipeId<<endl;
		cout <<"Output Pipe: "<<node->outPipeId<<endl;
		cout <<"Output Schema: " <<endl;
		node->outputSchema->Print();
		cout <<"\n";
		break;
	case WRITEOUT:
		cout <<"*****************"<<endl;
		cout <<"WRITE OUT"<<endl;
		cout <<"Input Pipe	"<<node->lPipeId<<endl;
		cout <<"Output Pipe "<<node->outFile<<endl;
		cout <<"Output Schema: " <<endl;
		node->outputSchema->Print();
		cout <<"\n";
		break;
	default:
		break;
	}

	
}

int QueryTree::createTable(CreateTable *createTable) {
	DBFile *db = new DBFile;
	char dbpath[100];
	sprintf(dbpath, "%s%s.bin", dbfile_dir, createTable->tableName);
	SortInfo *info = new SortInfo;
	OrderMaker *om = new OrderMaker;
	if(createTable->type == SORTED) {
		NameList *sortAtt = createTable->sortAttrList;
		while(sortAtt) {
			AttrList *atts = createTable->attrList;
			int i=0;
			while(atts) {
				if(strcmp(sortAtt->name, atts->attr->attrName)){
					//got it
					om->whichAtts[om->numAtts] = i;
					om->whichTypes[om->numAtts] = (Type) atts->attr->type;
					om->numAtts++;
					break;
				}
				i++;
				atts = atts->next;
			}
			sortAtt = sortAtt->next;
		}
		info->myOrder = om;
		info->runLength = RUNLEN;
		db->Create(dbpath, sorted, (void*)info);
	} else
		db->Create(dbpath, heap, NULL );
	db->Close();
	return 1;
}

int QueryTree::insertFile(InsertFile *insertFile) {
	DBFile dbfile;
	char dbpath[100];
	sprintf(dbpath, "%s%s.bin", dbfile_dir, insertFile->tableName);
	dbfile.Open(dbpath);
	char fpath[100];
	sprintf(fpath, "%s%s", tpch_dir, insertFile->fileName);
	cout <<"loading " <<fpath<<endl;
	Schema schema((char*)catalog_path, insertFile->tableName);
	dbfile.Load(schema, fpath);
	dbfile.Close();
	return 1;
}

int QueryTree::dropTable(char *dropTable) {
	char dbpath[100];
	sprintf(dbpath, "%s%s.bin", dbfile_dir, dropTable);
	remove(dbpath);
	sprintf(dbpath, "%s.header", dbpath);
	remove(dbpath);
	return 1;
}


void QueryTree::ExecuteNode(QueryTreeNode *node) {

	if(node->left)
		ExecuteNode(node->left);

	if(node->right)
		ExecuteNode(node->right);

	//last run it self
	//first new pipe, put it into the pipes
	switch(node->opType) {
	case SELECTFILE:
	{
		SelectFile *selectFile = new SelectFile();
		Pipe *sfOutPipe = new Pipe(PIPE_SIZE);
		//add it to the pipe
		this->pipes[node->outPipeId] = sfOutPipe;
		dbs[this->dbNum] = new DBFile;
		dbs[this->dbNum]->Open((char*)node->dbfilePath.c_str());
		dbs[this->dbNum]->MoveFirst();
		selectFile->Run(*(dbs[this->dbNum++]), *sfOutPipe, *(node->cnf), *(node->literal));
		break;
	}
	case SELECTPIPE:
	{
		SelectPipe *selectPipe = new SelectPipe();
		selectPipe->Use_n_Pages(RUNLEN);
		Pipe *spOutPipe = new Pipe(PIPE_SIZE);
		//add it to the pipe
		this->pipes[node->outPipeId] = spOutPipe;
		Pipe *splPipe = this->pipes[node->lPipeId];
		selectPipe->Run(*splPipe, *spOutPipe, *(node->cnf), *(node->literal));

		break;
	}
	case PROJECT:
	{
		Project *project = new Project;
		Pipe *pOutPipe = new Pipe(PIPE_SIZE);
		//add it to the pipe
		this->pipes[node->outPipeId] = pOutPipe;
		Pipe *plPipe = this->pipes[node->lPipeId];
		project->Run(*plPipe, *pOutPipe, node->keepMe, node->numAttsInput, node->numAttsOutput);
		project->WaitUntilDone();
		break;
	}
	case JOIN:
	{
		Join *join = new Join;
		Pipe *jOutPipe = new Pipe(PIPE_SIZE);
		//add it to the pipe
		this->pipes[node->outPipeId] = jOutPipe;
		Pipe *jlPipe = this->pipes[node->lPipeId];
		Pipe *jrPipe = this->pipes[node->rPipeId];
		join->Run(*jlPipe, *jrPipe, *jOutPipe, *(node->cnf), *(node->literal));
		join->WaitUntilDone();	
		break;
	}
	case SUM:
	{
		Sum *sum = new Sum;
		Pipe *sOutPipe = new Pipe(PIPE_SIZE);
		//add it to the pipe
		this->pipes[node->outPipeId] = sOutPipe;
		Pipe *slPipe = this->pipes[node->lPipeId];
		sum->Run(*slPipe, *sOutPipe, *(node->function));
		sum->WaitUntilDone();
		break;
	}
	case GROUP_BY:
	{
		GroupBy *groupBy = new GroupBy;
		Pipe *gbOutPipe = new Pipe(PIPE_SIZE);
		this->pipes[node->outPipeId] = gbOutPipe;
		Pipe *gblPipe = this->pipes[node->lPipeId];
		groupBy->Run(*gblPipe, *gbOutPipe, *(node->orderMaker), *(node->function));
		groupBy->WaitUntilDone();
		break;
	}
	case DISTINCT:
	{
		DuplicateRemoval *dr = new DuplicateRemoval;
		Pipe *drOutPipe = new Pipe(PIPE_SIZE);
		this->pipes[node->outPipeId] = drOutPipe;
		Pipe *drlPipe = this->pipes[node->lPipeId];
		dr->Run(*drlPipe, *drOutPipe, *(node->left->outputSchema));
		dr->WaitUntilDone();

		break;
	}
	case WRITEOUT:
	{
		WriteOut *wo = new WriteOut;
		Pipe *wlPipe = this->pipes[node->lPipeId];
		wo->Run(*wlPipe, node->outFile, *(node->outputSchema));
		this->operators.push_back(wo);
		break;
	}
	default:
		break;
	}
}

int QueryTree::executeQueryTree() {

	if( strcmp(this->output, "NONE") == 0) { //just print out the query plan
		this->PrintInOrder();
	} else {
		QueryTreeNode *writeOut = new QueryTreeNode;
		writeOut->opType = WRITEOUT;
		writeOut->left = this->root;
		writeOut->lPipeId = writeOut->left->outPipeId;
		writeOut->outputSchema = writeOut->left->outputSchema;
		if( strcmp(this->output, "STDOUT") == 0) {
			writeOut->outFile = stdout;
		} else {
			FILE *fp = fopen(this->output, "w");
			writeOut->outFile = fp;
		}
		this->ExecuteNode(writeOut);
		for(vector<RelationalOp *>::iterator roIt=this->operators.begin(); roIt!=this->operators.end();roIt++){
			RelationalOp *op = *roIt;
			op->WaitUntilDone();
		}
	}
	return 1;
}




