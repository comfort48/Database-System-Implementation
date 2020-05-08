#include "Statistics.h"

Statistics::Statistics()
{
	this->m_partitionNum = 0;
}
Statistics::Statistics(Statistics &copyMe)
{
	//a deep copy of unordered_map m_rel_stat;
	unordered_map<string, Rel_Stat *> *copyRel = copyMe.GetRelStat();
	unordered_map<string, Rel_Stat *>::iterator relIt;
	for (relIt = copyRel->begin(); relIt != copyRel->end(); relIt++)
	{
		Rel_Stat *rel = new Rel_Stat(relIt->second);
		this->m_rel_stat.insert(make_pair(relIt->first, rel));
	}

	//a deep copy of m_partition
	unordered_map<int, Partition *> *copyPart = copyMe.GetPartition();
	unordered_map<int, Partition *>::iterator partIt;
	for (partIt = copyPart->begin(); partIt != copyPart->end(); partIt++)
	{
		Partition *newP = new Partition(partIt->second);
		int newK = partIt->first;
		this->m_partition[newK] = newP;
	}

	this->m_partitionNum = copyMe.m_partitionNum;

	//a deep copy of m_partition
	unordered_map<string, vector<string> *> *copyAttToRel = copyMe.GetAttToRel();
	unordered_map<string, vector<string> *>::iterator a2rIt;
	for (a2rIt = copyAttToRel->begin(); a2rIt != copyAttToRel->end(); a2rIt++)
	{
		vector<string>::iterator vectIt;
		for (vectIt = a2rIt->second->begin(); vectIt != a2rIt->second->end(); vectIt++)
		{
			a2rIt->second->push_back(*vectIt);
		}
	}
}
Statistics::~Statistics()
{
	unordered_map<string, Rel_Stat *>::iterator rIt;
	for (rIt = m_rel_stat.begin(); rIt != m_rel_stat.end(); rIt++)
	{
		if (rIt->second)
			delete rIt->second;
	}
	m_rel_stat.clear();

	unordered_map<string, vector<string> *>::iterator aIt;
	for (aIt = m_att_to_rel.begin(); aIt != m_att_to_rel.end(); aIt++)
	{
		if (aIt->second)
			delete aIt->second;
	}
	m_att_to_rel.clear();

	unordered_map<int, Partition *>::iterator pIt;
	for (pIt = m_partition.begin(); pIt != m_partition.end(); pIt++)
	{
		if (pIt->second)
			delete pIt->second;
	}
	m_partition.clear();
}

int Statistics::ParseRelation(string name, string &rel)
{
	int prefixPos = name.find(".");
	if (prefixPos != string::npos)
	{
		//s.suppkey
		rel = name.substr(0, prefixPos);
	}
	else
	{
		//suppkey
		unordered_map<string, vector<string> *>::iterator crIt =
			this->m_att_to_rel.find(name);
		if (crIt == this->m_att_to_rel.end() || crIt->second->size() < 1)
		{
			cerr << "Atts: " << name << " can not find its relations." << endl;
			return 0;
		}
		vector<string> *rels = crIt->second;
		if (rels->size() <= 0)
		{
			cerr << "Atts: " << name << " does not belong to any relations!" << endl;
			return 0;
		}
		if (rels->size() == 1)
		{ //this att belong to only one relation
			rel = rels->at(0);
		}
		else
		{ //this att belong to more than one relations, like s.id, l.id, not happen in TPCH
			cerr << "Atts: " << name << " is ambiguous!" << endl;
			return 0;
		}
	}
	return 1;
}

void Statistics::AddRel(char *relName, int numTuples)
{
	unordered_map<string, Rel_Stat *>::iterator it =
		this->m_rel_stat.find(string(relName));
	if (it == this->m_rel_stat.end())
	{ //not found, add it
		Rel_Stat *rel = new Rel_Stat;
		rel->numTuples = numTuples;
		rel->numTuplesForWritten = numTuples;
		this->m_rel_stat[string(relName)] = rel;
	}
	else
	{ // found, update
		it->second->numTuples = numTuples;
		it->second->numTuplesForWritten = numTuples;
	}
	//	cout <<"add relation: " <<relName<<endl;
}

void Statistics::AddAtt(char *relName, char *attName, int numDistincts)
{
	unordered_map<string, Rel_Stat *>::iterator relIt = this->m_rel_stat.find(string(relName));
	if (relIt == this->m_rel_stat.end())
	{ // not found
		cerr << "relation " << relName << " not found!" << endl;
		exit(0);
	}
	Rel_Stat *rel = relIt->second;
	string attKey(attName);
	unsigned long int relNumDistinct = (numDistincts == -1) ? rel->numTuplesForWritten : numDistincts;
	unordered_map<string, unsigned long int>::iterator attIt = rel->atts.find(attKey);
	if (attIt == rel->atts.end())
	{ //not found, add
		rel->atts.insert(make_pair(attKey, relNumDistinct));
		//add the att to rel info
		vector<string> *relNames = new vector<string>;
		relNames->push_back(string(relName));
		this->m_att_to_rel.insert(make_pair(string(attName), relNames));
	}
	else
	{ //found, update
		attIt->second = relNumDistinct;
		//add or update the att to rel info
		unordered_map<string, vector<string> *>::iterator it = this->m_att_to_rel.find(string(attName));
		if (it == this->m_att_to_rel.end())
		{ //not found add
			vector<string> *relNames = new vector<string>;
			relNames->push_back(string(relName));
			this->m_att_to_rel.insert(make_pair(string(attName), relNames));
		}
		else
			it->second->push_back(string(relName));
	}
}
void Statistics::CopyRel(char *oldName, char *newName)
{
	unordered_map<string, Rel_Stat *>::iterator oIt = this->m_rel_stat.find(string(oldName));
	if (oIt == this->m_rel_stat.end())
	{ // old relation not found
		cerr << "relation " << oldName << " not found!" << endl;
		exit(0);
	}
	else
	{ //found
		Rel_Stat *new_rel = new Rel_Stat(oIt->second);
		this->m_rel_stat.insert(make_pair(string(newName), new_rel));
		//		cout <<"copy " <<oldName<< " as " <<newName<<" attNum "<<new_rel->atts.begin()->first<<endl;

		//add all atts of old relation into the attToRel info
		//		unordered_map <string, unsigned long int>::iterator attIt;
		//		unordered_map <string, vector<string>*>::iterator a2rIt;
		//		for(attIt=oIt->second->atts.begin();attIt!=oIt->second->atts.end();attIt++) {
		//			a2rIt = this->m_att_to_rel.find(attIt->first);
		//			if(a2rIt==this->m_att_to_rel.end()){
		//				cerr <<"attribute "<<attIt->first<<" not found!"<<endl;
		//				exit(0);
		//			} else {  // found, normal
		//				 a2rIt->second->push_back(string(newName));
		//			}
		//		}
	}
}

void Statistics::Read(char *fromWhere)
{
	//retrieve m_rel_stat and m_att_to_rel
	FILE *frp = fopen(fromWhere, "r");
	if (frp == NULL)
	{
		return;
	}

	// other variables
	char relName[200], attName[200];
	unsigned long int numValues;
	// make iterator for m_mColToTable map
	unordered_map<string, vector<string> *>::iterator crIt;

	while (fscanf(frp, "%s", relName) != EOF)
	{
		if (strcmp(relName, "RELATION") == 0)
		{
			//			Rel_Stat *rel = new Rel_Stat;
			fscanf(frp, "%s", relName);
			fscanf(frp, "%lu", &numValues);

			unordered_map<string, Rel_Stat *>::iterator rIt = this->m_rel_stat.find(string(relName));
			if (rIt == this->m_rel_stat.end())
				this->AddRel(relName, numValues);

			fscanf(frp, "%s", attName);
			while (strcmp(attName, "END") != 0)
			{
				fscanf(frp, "%lu", &numValues);
				this->AddAtt(relName, attName, numValues);
				//				rel->atts[attName] = numValues;

				//				crIt = this->m_att_to_rel.find(attName);
				//				if (crIt == this->m_att_to_rel.end()) // not found, add
				//				{
				//					vector<string> * atr = new vector<string> ;
				//					atr->push_back(relName);
				//					this->m_att_to_rel[attName] = atr;
				//				}
				//				else // found, add table to vector
				//					(crIt->second)->push_back(relName);

				// read next row
				fscanf(frp, "%s", attName);
			}
		}
	}
	//	cout <<"get from file relation num: " <<this->m_rel_stat.size()<<endl;
	fclose(frp);
}

void Statistics::Write(char *fromWhere)
{
	FILE *fwp = fopen(fromWhere, "w");
	for (unordered_map<string, Rel_Stat *>::iterator rIt = this->m_rel_stat.begin();
		 rIt != this->m_rel_stat.end(); rIt++)
	{
		Rel_Stat *r = rIt->second;
		if (!r->copied)
		{
			//not copied, write out
			fprintf(fwp, "RELATION\n");
			fprintf(fwp, "%s %lu\n", rIt->first.c_str(), r->numTuplesForWritten);
			for (unordered_map<string, unsigned long int>::iterator aIt = r->atts.begin();
				 aIt != r->atts.end(); aIt++)
			{
				//write out its att
				fprintf(fwp, "%s %lu\n", aIt->first.c_str(), aIt->second);
			}
			fprintf(fwp, "END\n");
		}
	}
	fclose(fwp);
}

void Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{
	//Estimate already did the error checking, if it is correctly calculated, then everything is OK.
	//And have the join_rels to install the joining relations
	unsigned long int result = (unsigned long int)this->Estimate(parseTree, relNames, numToJoin);

	//	cout <<"Joining relations num: " << this->join_rels.size()<<endl;
	if (this->join_rels.size() == 0)
	{ //---selection, no join---
		unordered_map<string, Rel_Stat *>::iterator rIt = this->m_rel_stat.find(string(relNames[0]));
		if (rIt == this->m_rel_stat.end())
		{
			cerr << "Relation " << relNames[0] << " can not be found!" << endl;
			exit(0);
		}
		Rel_Stat *rel = rIt->second;
		rel->numTuples = result;
		return;
	}
	else
	{
		/*
		 * Several relations to be joined.
		 * 1. If they belong to the same partition;
		 * 2. They belong to different partitions.
		 */
		set<int> join_part;
		vector<string> single_rel;
		for (vector<string>::iterator jrIt = this->join_rels.begin(); jrIt != this->join_rels.end(); jrIt++)
		{
			string relName = *jrIt;
			unordered_map<string, Rel_Stat *>::iterator rIt = this->m_rel_stat.find(relName);
			Rel_Stat *rel = rIt->second;
			if (rel->partition == -1)
				single_rel.push_back(relName);
			else
				join_part.insert(rel->partition);
		}

		//1. belong to the same partition
		if (single_rel.size() == 0 && join_part.size() == 1)
		{
			unordered_map<int, Partition *>::iterator jpIt =
				this->m_partition.find(*join_part.begin());
			Partition *jp = jpIt->second;
			jp->numTuples = result;
		}
		else
		{ // 2. different partition
			Partition *newPart = new Partition;
			newPart->numTuples = result;
			//add the singleton_relations
			for (vector<string>::iterator rsIt = single_rel.begin(); rsIt != single_rel.end(); rsIt++)
			{
				newPart->relations->push_back(*rsIt);
				unordered_map<string, Rel_Stat *>::iterator rIt = this->m_rel_stat.find(*rsIt);
				Rel_Stat *rel = rIt->second;
				rel->partition = this->m_partitionNum;
			}
			//add the relations in partition
			for (set<int>::iterator spIt = join_part.begin(); spIt != join_part.end(); spIt++)
			{
				unordered_map<int, Partition *>::iterator pIt =
					this->m_partition.find(*spIt);
				if (pIt == this->m_partition.end())
				{
					cerr << "Partition " << *spIt << " not found!" << endl;
					exit(0);
				}
				Partition *op = pIt->second;
				for (vector<string>::iterator roIt = op->relations->begin(); roIt != op->relations->end(); roIt++)
				{
					newPart->relations->push_back(*roIt);
					unordered_map<string, Rel_Stat *>::iterator rIt = this->m_rel_stat.find(*roIt);
					Rel_Stat *rel = rIt->second;
					rel->partition = this->m_partitionNum;
				}
			}
			this->m_partition.insert(make_pair(this->m_partitionNum, newPart));
			this->m_partitionNum++;
		}
	}
	//	cout <<"apply done, has partition num: " <<this->m_partitionNum<<endl;
	//	for(unordered_map <string, Rel_Stat *>::iterator it=this->m_rel_stat.begin(); it!=this->m_rel_stat.end();it++) {
	//		cout <<"Relation "<<it->first<<" partition is: " <<it->second->partition<<endl;
	//	}
	//	unordered_map <int, Partition*>::iterator pit = this->m_partition.find(m_partitionNum-1);
	//	cout <<"partition " <<m_partitionNum-1<<" numtupels: "<<pit->second->numTuples<<endl;
}

unsigned long int Statistics::findNumDistinctsWithName(string name, char *relNames[], int numToJoin, bool join)
{
	string tableName;
	string attName = name;
	unordered_map<string, unsigned long int>::iterator aIt;
	int prefixPos = attName.find(".");
	if (prefixPos != string::npos)
	{
		//s.suppkey
		unordered_map<string, Rel_Stat *>::iterator rIt;
		tableName = attName.substr(0, prefixPos);
		attName = attName.substr(prefixPos + 1);
		rIt = this->m_rel_stat.find(tableName);
		if (rIt == this->m_rel_stat.end())
		{
			cerr << "relation " << tableName << " not found!" << endl;
			exit(0);
		}
		Rel_Stat *rel = rIt->second;
		aIt = rel->atts.find(attName);
		if (aIt == rel->atts.end())
		{
			cerr << "Att: " << attName << " not found in relation " << tableName << endl;
			exit(0);
		}
		if (join)
			this->join_rels.push_back(tableName);
	}
	else
	{
		//suppkey
		unordered_map<string, vector<string> *>::iterator crIt =
			this->m_att_to_rel.find(attName);
		if (crIt == this->m_att_to_rel.end() || crIt->second->size() < 1)
		{
			cerr << "Atts: " << attName << " can not find its relations." << endl;
			exit(0);
		}
		vector<string> *rels = crIt->second;
		if (rels->size() <= 0)
		{
			cerr << "Atts: " << attName << " does not belong to any relations!" << endl;
			exit(0);
		}
		if (rels->size() == 1)
		{ //this att belong to only one relation
			Rel_Stat *rel = this->m_rel_stat.find(rels->at(0))->second;
			aIt = rel->atts.find(attName);
			if (aIt == rel->atts.end())
			{
				cerr << "Atts: " << attName << " not found in relation " << tableName << endl;
				exit(0);
			}
			if (join)
				this->join_rels.push_back(rels->at(0));
		}
		else
		{ //this att belong to more than one relations, like s.id, l.id, not happen in TPCH
			for (vector<string>::iterator it = rels->begin(); it != rels->end(); it++)
			{
				for (int i = 0; i < numToJoin; i++)
				{
					string r(relNames[i]);
					if (r.compare(*it) == 0)
					{ //match
						Rel_Stat *rel = this->m_rel_stat.find(rels->at(0))->second;
						aIt = rel->atts.find(attName);
						if (aIt == rel->atts.end())
						{
							cerr << "Atts: " << attName << " not found in relation " << tableName << endl;
							exit(0);
						}
						if (join)
							this->join_rels.push_back(r);
						return aIt->second;
					}
				}
			}
		}
	}
	return aIt->second;
}

double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin)
{
	/* First scan the relNames, find out their partitions.
	 * Scan the parserTree to scan the attributes, find out the factors
	 * Calculate the result according the joining partitions, and facotrs
	 */
	this->join_rels.clear();

	if (numToJoin == 0)
	{
		cerr << "no relation to be estimated!" << endl;
		return 0;
	}

	double result = 1;

	//	cout <<"relation num: " <<this->m_rel_stat.size()<<endl;
	/*	set <int> join_partitions;
	int numRels = 0;
	for(int i = 0; i< numToJoin; i++) {
		unordered_map <string, Rel_Stat *>::iterator rIt =
				this->m_rel_stat.find(string(relNames[i]));
		if(rIt == this->m_rel_stat.end()) {
			cerr <<"Relation " << relNames[i] << " can not be found!" <<endl;
			exit(0);
		}
		Rel_Stat *rel = rIt->second;
		if(rel->partition == -1) { //singleton
			cout <<"relation " <<relNames[i]<<" has tuples "<<rel->numTuples<<endl;
			result *= rel->numTuples;
			numRels ++;
		} else {  //sets'
			join_partitions.insert(rel->partition);
		}
	}
	for(set<int>::iterator sIt = join_partitions.begin(); sIt!=join_partitions.end(); sIt++) {
		// partition
		unordered_map <int, Partition*>::iterator pIt = this->m_partition.find(*sIt);
		Partition * p = pIt->second;
		numRels += p->relations->size();
		result *= p->numTuples;
	}
	if(numRels != numToJoin) {
		//join relations has abnormality
		cerr <<"Join can not be made"<<endl;
		exit(0);
	}

//	cout <<"Join is OK in terms of relation: "<<result<<endl;
*/
	//--------scanning the attributes----get the factors
	if (!parseTree)
	{				   // parseTree is null
		return result; //no matter it is an selection or join
	}

	string dummyAtt;
	vector<double> andFactors;
	while (parseTree)
	{
		//		cout <<"get another and list"<<endl;
		OrList *leftOr = parseTree->left;
		parseTree = parseTree->rightAnd;
		vector<unsigned long int> orFactors;
		bool sameAtt = false;
		string pilotAtt = "";
		while (leftOr)
		{
			ComparisonOp *comOp = leftOr->left;
			//			cout <<"get another orlist"<<endl;
			leftOr = leftOr->rightOr;

			//now we process this comOp
			Operand *left = comOp->left;
			Operand *right = comOp->right;
			unsigned long int num1, num2;
			if (left->code == NAME && right->code == NAME)
			{ //-------- join condition ------
				//att1 = att2
				//this is an join condition, first parse s.suppkey
				num1 = findNumDistinctsWithName(string(left->value), relNames, numToJoin, true);
				num2 = findNumDistinctsWithName(string(right->value), relNames, numToJoin, true);

				//				unordered_map <string, Rel_Stat *>::iterator rIt = this->m_rel_stat.find(string(left->value));
				//				if()
				//				cout <<"result:: " <<result<<endl;
				//				cout <<num1 <<" and "<<num2<<endl;
				num1 >= num2 ? (orFactors.push_back(num1)) : (orFactors.push_back(num2));

				continue;
			}
			if (left->code == NAME)
			{
				// att1=2, att<2, att>2
				if (pilotAtt.compare(left->value) == 0)
					sameAtt = true;
				else
					pilotAtt = left->value;

				if (comOp->code == EQUALS)
				{
					num1 = findNumDistinctsWithName(string(left->value), relNames, numToJoin, false);
					orFactors.push_back(num1);
				}
				else
					orFactors.push_back(3);
				dummyAtt = string(left->value);
				continue;
			}
			if (right->code == NAME)
			{
				if (pilotAtt.compare(right->value) == 0)
					sameAtt = true;
				else
					pilotAtt = left->value;

				if (comOp->code == EQUALS)
				{
					num2 = findNumDistinctsWithName(string(right->value), relNames, numToJoin, false);
					orFactors.push_back(num2);
				}
				else
					orFactors.push_back(3);
				dummyAtt = string(right->value);
				continue;
			}
		}

		if (orFactors.size() < 1)
			continue;
		//		cout <<"Orfacotr size: " <<orFactors.size()<<endl;
		//		for(vector<unsigned long int>::iterator it=orFactors.begin(); it!=orFactors.end();it++)
		//			cout <<*it<<" ";
		//		cout <<endl;
		double factor = 0;
		if (sameAtt)
		{
			for (vector<unsigned long int>::iterator it = orFactors.begin(); it != orFactors.end(); it++)
			{
				unsigned long int f = *it;
				factor += (double)1 / f;
			}
			//			cout <<"same att"<<factor<<endl;
		}
		else
		{
			factor = 1;
			for (vector<unsigned long int>::iterator it = orFactors.begin(); it != orFactors.end(); it++)
			{
				unsigned long int f = *it;
				factor *= (1 - (double)1 / f);
			}
			factor = 1 - factor;
		}
		//		cout <<"the factor is: " <<factor<<endl;
		if (factor)
			andFactors.push_back(factor);
	}

	//		cout <<"Joining relations: " <<this->join_rels.size()<<endl;
	if (this->join_rels.size() == 0)
	{
		//doing an selection
		//find out the relations with dummyAtt
		//		cout <<"dummyatt "<<dummyAtt<<endl;
		vector<string> *dummyRels = this->m_att_to_rel[dummyAtt];
		//		cout <<"dummi 0 "<< *(dummyRels->begin())<<endl;
		Rel_Stat *sr = m_rel_stat[*(dummyRels->begin())];
		if (sr->partition == -1)
			result *= sr->numTuples;
		else
		{
			Partition *p = this->m_partition[sr->partition];
			if (p->relations->size() > numToJoin)
			{
				cerr << "Relations can not be joined!" << endl;
				exit(0);
			}
			result *= p->numTuples;
		}
		//		cout <<"tme result: " <<result<<endl;
	}
	else
	{
		int numRels = 0;
		for (vector<string>::iterator jrIt = this->join_rels.begin(); jrIt != this->join_rels.end(); jrIt++)
		{
			Rel_Stat *rs = this->m_rel_stat[*jrIt];
			if (rs->partition == -1)
			{
				result *= rs->numTuples;
				numRels++;
			}
			else
			{
				Partition *p = this->m_partition[rs->partition];
				result *= p->numTuples;
				numRels += p->relations->size();
			}
		}
		if (numRels > numToJoin)
		{
			cerr << "Relations can not be joined!" << endl;
			exit(0);
		}
	}

	for (vector<double>::iterator fit = andFactors.begin(); fit != andFactors.end(); fit++)
	{
		result *= *fit;
	}
	return result;
}

void Statistics::loadStatistics()
{
	char *relName[] = {(char *)"supplier", (char *)"partsupp", (char *)"lineitem",
					   (char *)"orders", (char *)"customer", (char *)"nation", (char *)"part", (char *)"region"};
	AddRel(relName[0], 10000);	 //supplier
	AddRel(relName[1], 800000);	 //partsupp
	AddRel(relName[2], 6001215); //lineitem
	AddRel(relName[3], 1500000); //orders
	AddRel(relName[4], 150000);	 //customer
	AddRel(relName[5], 25);		 //nation
	AddRel(relName[6], 200000);	 //part
	AddRel(relName[7], 5);		 //region
						 //	    AddRel(relName[8], 3);        //mal_test

	AddAtt(relName[0], (char *)"s_suppkey", 10000);
	AddAtt(relName[0], (char *)"s_nationkey", 25);
	AddAtt(relName[0], (char *)"s_acctbal", 9955);
	AddAtt(relName[0], (char *)"s_name", 100000);
	AddAtt(relName[0], (char *)"s_address", 100000);
	AddAtt(relName[0], (char *)"s_phone", 100000);
	AddAtt(relName[0], (char *)"s_comment", 10000);

	AddAtt(relName[1], (char *)"ps_suppkey", 10000);
	AddAtt(relName[1], (char *)"ps_partkey", 200000);
	AddAtt(relName[1], (char *)"ps_availqty", 9999);
	AddAtt(relName[1], (char *)"ps_supplycost", 99865);
	AddAtt(relName[1], (char *)"ps_comment", 799123);

	AddAtt(relName[2], (char *)"l_returnflag", 3);
	AddAtt(relName[2], (char *)"l_discount", 11);
	AddAtt(relName[2], (char *)"l_shipmode", 7);
	AddAtt(relName[2], (char *)"l_orderkey", 1500000);
	AddAtt(relName[2], (char *)"l_receiptdate", 0);
	AddAtt(relName[2], (char *)"l_partkey", 200000);
	AddAtt(relName[2], (char *)"l_suppkey", 10000);
	AddAtt(relName[2], (char *)"l_linenumbe", 7);
	AddAtt(relName[2], (char *)"l_quantity", 50);
	AddAtt(relName[2], (char *)"l_extendedprice", 933900);
	AddAtt(relName[2], (char *)"l_tax", 9);
	AddAtt(relName[2], (char *)"l_linestatus", 2);
	AddAtt(relName[2], (char *)"l_shipdate", 2526);
	AddAtt(relName[2], (char *)"l_commitdate", 2466);
	AddAtt(relName[2], (char *)"l_shipinstruct", 4);
	AddAtt(relName[2], (char *)"l_comment", 4501941);

	AddAtt(relName[3], (char *)"o_custkey", 150000);
	AddAtt(relName[3], (char *)"o_orderkey", 1500000);
	AddAtt(relName[3], (char *)"o_orderdate", 2406);
	AddAtt(relName[3], (char *)"o_totalprice", 1464556);
	AddAtt(relName[3], (char *)"o_orderstatus", 3);
	AddAtt(relName[3], (char *)"o_orderpriority", 5);
	AddAtt(relName[3], (char *)"o_clerk", 1000);
	AddAtt(relName[3], (char *)"o_shippriority", 1);
	AddAtt(relName[3], (char *)"o_comment", 1478684);

	AddAtt(relName[4], (char *)"c_custkey", 150000);
	AddAtt(relName[4], (char *)"c_nationkey", 25);
	AddAtt(relName[4], (char *)"c_mktsegment", 5);
	AddAtt(relName[4], (char *)"c_name", 150000);
	AddAtt(relName[4], (char *)"c_address", 150000);
	AddAtt(relName[4], (char *)"c_phone", 150000);
	AddAtt(relName[4], (char *)"c_acctbal", 140187);
	AddAtt(relName[4], (char *)"c_comment", 149965);

	AddAtt(relName[5], (char *)"n_nationkey", 25);
	AddAtt(relName[5], (char *)"n_regionkey", 5);
	AddAtt(relName[5], (char *)"n_name", 25);
	AddAtt(relName[5], (char *)"n_comment", 25);

	AddAtt(relName[6], (char *)"p_partkey", 200000);
	AddAtt(relName[6], (char *)"p_size", 50);
	AddAtt(relName[6], (char *)"p_container", 40);
	AddAtt(relName[6], (char *)"p_name", 199996);
	AddAtt(relName[6], (char *)"p_mfgr", 5);
	AddAtt(relName[6], (char *)"p_brand", 25);
	AddAtt(relName[6], (char *)"p_type", 150);
	AddAtt(relName[6], (char *)"p_retailprice", 20899);
	AddAtt(relName[6], (char *)"p_comment", 127459);

	AddAtt(relName[7], (char *)"r_regionkey", 5);
	AddAtt(relName[7], (char *)"r_name", 5);
	AddAtt(relName[7], (char *)"r_comment", 5);
	

	//	    AddAtt(relName[8], "col1",3);
	//	    AddAtt(relName[8], "col2",6);
	//	    AddAtt(relName[8], "col3",6);
	//	    AddAtt(relName[8], "col4",6);
}
