#include "Statistics.h"
#include <set>

Statistics::Statistics()
{
    tableNames["n"] = "nation";
    tableNames["r"] = "region";
    tableNames["p"] = "part";
    tableNames["l"] = "lineitem";
    tableNames["o"] = "orders";
    tableNames["c"] = "customer";
    tableNames["ps"] = "partsupp";
    tableNames["s"] = "supplier";
}
Statistics::Statistics(Statistics &copyMe)
{
    this->relationSets = copyMe.relationSets;
    this->listOfRelations = copyMe.listOfRelations;
    this->tableNames = copyMe.tableNames;
}

Statistics& Statistics::operator= (Statistics& assignMe) {

    if(&assignMe != this) {
        listOfRelations = assignMe.listOfRelations;
        relationSets = assignMe.relationSets;
        tableNames = assignMe.tableNames;
    }
    return *this;
}

void Statistics::AddRel(char *relName, int numTuples)
{
    // function to add the relation with its num of tuples to the hash structure for lookup
    string relString(relName);

    if(listOfRelations.find(relString) != listOfRelations.end()) {
        listOfRelations[relString].updateNumberOfRowsCount((double)numTuples);
    }
    else{
        RelationStatistics tempStatistics((double)numTuples);
        RelationStatSet tempStatSet(relString);
        listOfRelations[relString] = tempStatistics;

        relationSets[relString] = tempStatSet;
        listOfSets.push_back(tempStatSet);
    }
}

void Statistics::AddRel(char *relName, double numTuples)
{
    // function to add the relation with its num of tuples to the hash structure for lookup
    string relString(relName);

    if(listOfRelations.find(relString) != listOfRelations.end()) {
        listOfRelations[relString].updateNumberOfRowsCount(numTuples);
    }
    else{
        RelationStatistics tempStatistics(numTuples);
        RelationStatSet tempStatSet(relString);
        listOfRelations[relString] = tempStatistics;

        relationSets[relString] = tempStatSet;
        listOfSets.push_back(tempStatSet);
    }
}

void Statistics::AddAtt(char *relName, char *attName, int numDistincts)
{
    // function to add the relation with its attributes and number distinct tuple to hash structure for faster lookup
    string relString(relName);
    string attrName(attName);

    if(listOfRelations.find(relString) != listOfRelations.end()) {
        listOfRelations[relString].addAttribute(attrName, numDistincts);
    }
}


void Statistics::CopyRel(char *oldName, char *newName)
{
    //cout<<"***********************************"<<endl;
    // function to create relation and store it under a new name
    string _oldName(oldName), _newName(newName);
    
    if(listOfRelations.find(_oldName) != listOfRelations.end()) {
        listOfRelations[_newName] = listOfRelations[_oldName];
        RelationStatSet newStatSet(newName);
        relationSets[_newName] = newStatSet;
        listOfSets.push_back(newStatSet);
    }
}

void Statistics::Read(char *fromWhere)
{
    ifstream fileReader(fromWhere);

    if(fileReader.is_open()) {

        stringstream ss;
        string inputLine;
        string relString;

        int numberOfSets = 0, numberOfStats = 0;

        if(fileReader.good()) {
            getline(fileReader, inputLine);
            ss.str(inputLine);
            if(!(ss>>numberOfStats)) {
                numberOfStats = 0;
            }
            ss.clear();
            ss.str("");

            for(int i = 0; i < numberOfStats; i++) {
                RelationStatistics tempStats;
                getline(fileReader, inputLine);
                if(inputLine.compare("") == 0) {
                    getline(fileReader, inputLine);
                }
                relString = inputLine;
                fileReader>>tempStats;
                listOfRelations[relString] = tempStats;
            }
            getline(fileReader, inputLine);
            while(inputLine.compare("#####") != 0) {
                getline(fileReader, inputLine);
            }
            getline(fileReader, inputLine);
            getline(fileReader, inputLine);
            ss.str(inputLine);
            if(!(ss>>numberOfSets)) {
                numberOfSets = 0;
            }
            getline(fileReader, inputLine);

            for(int i = 0; i < numberOfSets; i++) {
                RelationStatSet tempRelSet;
                vector<string> setRelVec;

                fileReader>>tempRelSet;
                tempRelSet.getRelations(setRelVec);
                listOfSets.push_back(tempRelSet);
                for(vector<string>::iterator it = setRelVec.begin(); it != setRelVec.end(); it++) {
                    relationSets[*it] = tempRelSet;
                } 
            }
        }
        fileReader.close();
    }
}

void Statistics::Write(char *fromWhere)
{
    ofstream fileWriter(fromWhere);

    if(fileWriter.is_open() && listOfSets.size() > 0) {
        fileWriter<<listOfRelations.size()<<endl<<endl;
        for(map<string, RelationStatistics>::iterator itr = listOfRelations.begin(); itr != listOfRelations.end(); itr++) {
            if(fileWriter.good()) {
                fileWriter<<itr->first<<endl<<itr->second<<endl;
            }
        }
        fileWriter<<"#####\n"<<endl;
        fileWriter<<listOfSets.size()<<endl<<endl;

        for(int i = 0; i < (int)listOfSets.size(); i++) {
            RelationStatSet rs = listOfSets[i];
            fileWriter<<rs<<endl;
        }
        fileWriter.close();
    }    
}

void Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{
    // as Apply is same as the estimate operation calling apply using a flag isApply
    RelationStatSet tempSet;
    vector<int> indexVec;

    vector<RelationStatSet> copyVector;

    for(int i = 0; i < numToJoin; i++) {
        tempSet.addRelationToSet(relNames[i]);
    }

    double joinEstimate = 0.0;
    joinEstimate = parseJoin(parseTree);

    double estimate = guessEstimate(parseTree, tempSet, indexVec, joinEstimate);
    int index = 0;
    int oldSize = listOfSets.size();
    int newSize = oldSize - (int)indexVec.size() + 1;

    if(indexVec.size() > 1) {
        for(int i = 0; i < oldSize; i++) {
            if(indexVec[index] == i) {
                index++;
            }
            else {
                copyVector.push_back(listOfSets[i]);
            }
        }
        listOfSets.clear();
        for(int i = 0; i < newSize - 1; i++) {
            listOfSets.push_back(copyVector[i]);
        }
        tempSet.updateNumberOfTuples(estimate);
        listOfSets.push_back(tempSet);
        copyVector.clear();

        vector<string> listOfRelations;
        tempSet.getRelations(listOfRelations);
        for(int i = 0; i < tempSet.size(); i++) {
            relationSets[listOfRelations[i]] = tempSet;
        }
    }
}

double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin)
{
    //cout<<"INNNNNNNN Estimate"<<endl;
    double estimate = 0.0;
    RelationStatSet relationSet;
    vector<int> indexVector;

    for(int i = 0; i < numToJoin; i++) {
        //cout<<"Issue in for Loop?"<<endl;
        relationSet.addRelationToSet(relNames[i]);
    }
    double joinEstimation = 0.0;
    //cout<<"IS the issue in parseJoin?"<<endl;
    joinEstimation = parseJoin(parseTree);
    
    estimate = guessEstimate(parseTree, relationSet, indexVector, joinEstimation);
    return estimate;
}

double Statistics::guessEstimate(struct AndList* parseTree, RelationStatSet toEstimate, vector<int> &indexVector, double joinEstimate) {

    double estimate = 0.0;

    int numberOfRelations = 0;
    int intersection = 0;
    int size = (int)listOfSets.size();

    for(int index = 0; index < size; index++) {
        intersection = listOfSets[index].intersect(toEstimate);
        if(intersection == -1) {
            indexVector.clear();
            numberOfRelations = 0;
            break;
        }
        else if(intersection > 0) {
            numberOfRelations = numberOfRelations + intersection;
            indexVector.push_back(index);
            if(numberOfRelations == toEstimate.size()) {
                break;
            }
        }
    }

    if(numberOfRelations > 0) {
        
        estimate = 1.0;
        
        if(joinEstimate > 0) {
            estimate = joinEstimate;
        }
        struct AndList* currentAnd = parseTree;
        struct OrList* currentOr;
        struct ComparisonOp* currentOperator;

        RelationStatistics r1;

        string rel1;
        string attr1;

        bool hasJoin = false;
        long numberOfTuples = 0.0l;

        while(currentAnd) {
            currentOr = currentAnd->left;
            struct OrList* tmpCurrentOr = currentOr;
            string lName;
            vector<string> checkVector;

            while(tmpCurrentOr) {
                if(checkVector.size() == 0) {
                    lName = tmpCurrentOr->left->left->value;
                    checkVector.push_back(lName);
                }
                else{
                    if(checkVector[0].compare(tmpCurrentOr->left->left->value) != 0) {
                        lName = tmpCurrentOr->left->left->value;
                        checkVector.push_back(lName);
                    }
                }
                tmpCurrentOr = tmpCurrentOr->rightOr;
            }
            bool independentOrVal = checkVector.size() > 1;
            bool singleOrVal = currentOr->rightOr == NULL;

            double tmpOr = 0.0;
            if(independentOrVal)
                tmpOr = 1.0;
            
            while(currentOr) {
                currentOperator = currentOr->left;
                Operand* op = currentOperator->left;
                if(op->code != NAME) {
                    op = currentOperator->right;
                }
                parseRelationAndAttribute(op, rel1, attr1);
                r1 = listOfRelations[rel1];

                if(currentOperator->code == EQUALS) {
                    if(singleOrVal) {
                        if(currentOperator->right->code == NAME && currentOperator->left->code == NAME) {
                            tmpOr = 1.0;
                            hasJoin = true;
                        }
                        else {
                            double const cal = (1.0l / r1[attr1]);
                            tmpOr += cal;
                        }
                    }
                    else {
                        if(independentOrVal) {
                            double const cal = 1.0l-(1.0l/r1[attr1]);
                            tmpOr *= cal;
                        }
                        else {
                            double const cal = 1.0l / r1[attr1];
                            tmpOr += cal;
                        }
                    }
                }
                else {
                    if(singleOrVal) {
                        double const cal = 1.0l / 3.0l;
                        tmpOr += cal;
                    }
                    else {
                        if(independentOrVal){
                            double cal = 1.0l-(1.0l/3.0l);
                            tmpOr *= cal;
                        }
                        else {
                            double cal = 1.0l / 3.0l;
                            tmpOr += cal;
                        }
                    }
                }
                if(!hasJoin) {
                    if(relationSets[rel1].getNumberOfTuples() == -1) {
                        numberOfTuples = r1.getNumberOfRows();
                    }
                    else {
                        numberOfTuples = relationSets[rel1].getNumberOfTuples();
                    }
                }
                currentOr = currentOr->rightOr;
            }
            if(singleOrVal) {
                estimate *= tmpOr;
            }
            else {
                if(independentOrVal) {
                    estimate *= (1-tmpOr);
                }
                else {
                    estimate *= tmpOr;
                }
            }
            currentAnd = currentAnd->rightAnd;
        }
        if(!hasJoin){
            estimate = numberOfTuples * estimate;
        }
    }
    return estimate;
}

double Statistics::parseJoin(struct AndList *parseTree) {
    double val = 0.0, temp = 0.0;
    //cout<<"Entered ParseJoin"<<endl;
    if(parseTree) {
        struct AndList *currentAnd = parseTree;
        struct OrList *currentOr;
        struct ComparisonOp *currentOperator;

        bool finish = false;
        string rel1, rel2, rel3, attr1, attr2;
        RelationStatistics relStats1, relStats2;
        //cout<<"In the if of parseJoin"<<endl;
        while(currentAnd && !finish) {
            currentOr = currentAnd->left;
            while(currentOr && !finish) {
                currentOperator = currentOr->left;
                if(currentOperator) {
                    if(currentOperator->code == EQUALS && currentOperator->left->code == currentOperator->right->code) {
                        finish = true;
                        parseRelationAndAttribute(currentOperator->left, rel1, attr1);
                        parseRelationAndAttribute(currentOperator->right, rel2, attr2);

                        relStats1 = listOfRelations[rel1];
                        relStats2 = listOfRelations[rel2];
                        val = getCountOfTuples(rel1, temp) * getCountOfTuples(rel2, temp);
                        if(relStats1[attr1] >= relStats2[attr2]){
                            val = val / (double)relStats1[attr1];
                        }
                        else if (relStats1[attr1] < relStats2[attr2]){
                            val = val / (double)relStats2[attr2];
                        }
                    }
                }
                currentOr = currentOr->rightOr;
            }
            currentAnd = currentAnd->rightAnd;
        }
    }
    return val;
}

double Statistics::getCountOfTuples(string rel, double joinEstimate) {
    
    double retVal = joinEstimate;
    
    if(retVal == 0.0)
        retVal = relationSets[rel].getNumberOfTuples();
    if(retVal == -1.0)
        retVal = (double)listOfRelations[rel].getNumberOfRows();
    return retVal;

}

void Statistics::parseRelationAndAttribute(struct Operand *oprnd, string &rel, string &attr) {

    string val(oprnd->value);
    string relString;
    stringstream ss;
    int index = 0;
    
    while(val[index] != '_') {
        if(val[index] == '.') {
            rel = ss.str();
            break;
        }
        ss<<val[index];
        index++;
    }
    if(val[index] == '.')
        attr = val.substr(index+1);
    else
    {
        attr = val;
        relString = ss.str();
        rel = tableNames[relString];
    }
    
}

void Statistics::parseRelation(struct Operand* oprnd, string &rel) {

    string val(oprnd->value);
    string relString;
    stringstream ss;

    int index = 0;
    while(val[index] != '_') {
        if(val[index] == '.') {
            rel = ss.str();
            return;
        }
        ss<<val[index];
        index++;
    }
}

Statistics::~Statistics()
{
    // destructor for the statistics 
}