#include "Statistics.h"
#include <set>

Statistics::Statistics()
{
    isApply = false;
    relationDataStore = new string_to_int_mapper();
    attributeDataStore = new string_to_hashmap_mapper();
}
Statistics::Statistics(Statistics &copyMe)
{
    // copy constructor to create replica of the object
    relationDataStore = new string_to_int_mapper(*(copyMe.relationDataStore));
    attributeDataStore = new string_to_hashmap_mapper(*(copyMe.attributeDataStore));
}

void Statistics::AddRel(char *relName, int numTuples)
{
    // function to add the relation with its num of tuples to the hash structure for lookup
    string relNameAsString(relName);
    if (relationDataStore->find(relNameAsString) != relationDataStore->end())
    {
        cout << "Record Exits" << endl;
        relationDataStore->erase(relNameAsString);
        relationDataStore->insert(make_pair(relNameAsString, numTuples));
    }
    else
    {
        relationDataStore->insert(make_pair(relNameAsString, numTuples));
    }
}

void Statistics::AddAtt(char *relName, char *attName, int numDistincts)
{
    // function to add the relation with its attributes and number distinct tuple to hash structure for faster lookup
    string relNameAsString(relName), attNameAsString(attName);
    if (numDistincts != -1)
        (*attributeDataStore)[relNameAsString][attNameAsString] = numDistincts;
    else
    {
        int numTuples = relationDataStore->at(relNameAsString);
        (*attributeDataStore)[relNameAsString][attNameAsString] = numTuples;
    }
}
void Statistics::CopyRel(char *oldName, char *newName)
{
    // function to create relation and store it under a new name
    string _oldName(oldName), _newName(newName);
    int _oldNumberOfTuples = (*relationDataStore)[_oldName];
    // adding the relation with new name to hash structure
    (*relationDataStore)[_newName] = _oldNumberOfTuples;

    string_to_int_mapper &_oldAttributeData = (*attributeDataStore)[_oldName];
    // adding the attributes of that relation and store it to hash structure
    for (string_to_int_mapper::iterator _oldAttributeInfo = _oldAttributeData.begin(); _oldAttributeInfo != _oldAttributeData.end(); _oldAttributeInfo++)
    {
        string _newAttribute = _newName;
        _newAttribute += "." + _oldAttributeInfo->first;
        (*attributeDataStore)[_newName][_newAttribute] = _oldAttributeInfo->second;
    }
}

void Statistics::Read(char *fromWhere)
{
    string inputFileName(fromWhere);
    ifstream ifile(fromWhere);

    if (!ifile)
    {
        cout << "File doesn't exit!";
        return;
    }

    ifstream inputFileReader;
    inputFileReader.open(inputFileName.c_str(), ios::in);

    string input;
    inputFileReader >> input;
    int relationDataStoreSize = atoi(input.c_str());
    relationDataStore->clear();
    // reading the relation data from the statistics file
    while (relationDataStoreSize--)
    {
        inputFileReader >> input;
        size_t splitPosition = input.find_first_of("#");
        string leftPart = input.substr(0, splitPosition);
        string rightPart = input.substr(splitPosition + 1);
        int rightPartAsInt = atoi(rightPart.c_str());
        (*relationDataStore)[leftPart] = rightPartAsInt;
    }

    inputFileReader >> input;

    attributeDataStore->clear();

    string relationName, attributeName, numDistincts;
    inputFileReader >> relationName >> attributeName >> numDistincts;
    // reading relation attribute data from the statistics file
    while (!inputFileReader.eof())
    {
        int numDistinctsInt = atoi(numDistincts.c_str());
        (*attributeDataStore)[relationName][attributeName] = numDistinctsInt;
        inputFileReader >> relationName >> attributeName >> numDistincts;
    }

    inputFileReader.close();
}

void Statistics::Write(char *fromWhere)
{
    remove(fromWhere);
    string fileName(fromWhere);

    ofstream outputFileWriter;

    outputFileWriter.open(fileName.c_str(), ios::out);

    int relationDataStoreSize = relationDataStore->size();
    // writing relation data in the Hash structure with its cout and corresponding tuple estimation cost
    outputFileWriter << relationDataStoreSize << endl;

    for (string_to_int_mapper::iterator itr = relationDataStore->begin(); itr != relationDataStore->end(); itr++)
    {
        outputFileWriter << itr->first.c_str() << "#" << itr->second << endl;
    }

    int counter = 0;
    for (string_to_hashmap_mapper::iterator i = attributeDataStore->begin(); i != attributeDataStore->end(); i++)
    {
        for (string_to_int_mapper::iterator j = i->second.begin(); j != i->second.end(); j++)
        {
            counter++;
        }
    }
    // writing relation data with attribute data in Hash Structure with its exisiting tuple count
    outputFileWriter << counter << endl;

    for (string_to_hashmap_mapper::iterator i = attributeDataStore->begin(); i != attributeDataStore->end(); i++)
    {
        for (string_to_int_mapper::iterator j = i->second.begin(); j != i->second.end(); j++)
        {
            outputFileWriter << (*i).first.c_str() << " " << (*j).first.c_str() << " " << (*j).second << endl;
        }
    }

    outputFileWriter.close();
}

void Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{
    // as Apply is same as the estimate operation calling apply using a flag isApply
    isApply = true;
    Estimate(parseTree, relNames, numToJoin);
    isApply = false;
}

double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin)
{
    double estimate = 0.0;
    struct AndList *andlist = parseTree;
    struct OrList *orlist;
    string leftRelation, rightRelation;
    string joinLeftRelation, joinRightRelation;
    double estimateORfraction = 1.0;
    double estimateANDfraction = 1.0;
    string leftAttr, rightAttr, prev;
    // Hash strucute for look up of the attributes which are based on attribute and a literal
    string_to_int_mapper relationOpMap;
    bool isdepth = false;
    bool done = false;
    bool isjoin = false;
    bool isjoinInvolved = false;
    // parsing the entire tree
    while (andlist != NULL)
    {

        orlist = andlist->left;
        estimateORfraction = 1.0;
        // parsing the tree on left for CNF
        while (orlist != NULL)
        {
            isjoin = false;
            ComparisonOp *currCompOp = orlist->left;

            string joinAttrleft, joinAttrright;
            // getting the info on the left side of a condition
            if (currCompOp->left->code == NAME)
            {

                leftAttr = currCompOp->left->value;

                if (strcmp(leftAttr.c_str(), prev.c_str()) == 0)
                {
                    isdepth = true;
                }
                prev = leftAttr;

                joinAttrleft = currCompOp->left->value;
                // searching in the attribute Hash structure for finding the left Relation
                for (string_to_hashmap_mapper::iterator mapEntry = attributeDataStore->begin(); mapEntry != attributeDataStore->end(); mapEntry++)
                {
                    if ((*attributeDataStore)[mapEntry->first].count(joinAttrleft) > 0)
                    {
                        leftRelation = mapEntry->first;
                        break;
                    }
                }
            }
            else
            {
                cout << "Not a valid CNF, left value should be an attribute" << endl;

                return 0;
            }
            // getting the info from right side of literal, condition is met only in case of join
            if (currCompOp->right->code == NAME)
            {
                // flag to mark join, used for estimation
                isjoin = true;
                // flag to mark if join is exisitng in entire CNF, used to apply the estimation to structure
                isjoinInvolved = true;
                joinAttrright = currCompOp->right->value;
                // searching in the attribute Hash structure for finding the right Relation
                for (string_to_hashmap_mapper::iterator mapEntry = attributeDataStore->begin(); mapEntry != attributeDataStore->end(); mapEntry++)
                {
                    if ((*attributeDataStore)[mapEntry->first].count(joinAttrright) > 0)
                    {
                        rightRelation = mapEntry->first;
                        break;
                    }
                }
            }
            if (isjoin == true)
            {
                // condition is join and the distinct tuple count is to used to find the Or fraction 
                double leftDistinctCount = (*attributeDataStore)[leftRelation][currCompOp->left->value];
                double rightDistinctCount = (*attributeDataStore)[rightRelation][currCompOp->right->value];

                if (currCompOp->code == EQUALS)
                {
                    estimateORfraction *= (1.0 - (1.0 / max(leftDistinctCount, rightDistinctCount)));
                }
                joinLeftRelation = leftRelation;
                joinRightRelation = rightRelation;
            }
            else
            {
                if (isdepth)
                {
                    // condition is met only if there is multiple CNF over a single left table with literals on right
                    if (!done)
                    {
                        estimateORfraction = 1.0 - estimateORfraction;
                        done = true;
                    }
                    if (currCompOp->code == GREATER_THAN || currCompOp->code == LESS_THAN)
                    {
                        estimateORfraction += (1.0 / 3.0);
                        relationOpMap[currCompOp->left->value] = currCompOp->code;
                    }
                    if (currCompOp->code == EQUALS)
                    {
                        estimateORfraction += (1.0 / ((*attributeDataStore)[leftRelation][currCompOp->left->value]));
                        relationOpMap[currCompOp->left->value] = currCompOp->code;
                    }
                }
                else
                {
                    // condition for only single CNF with an attribute and literal 
                    if (currCompOp->code == GREATER_THAN || currCompOp->code == LESS_THAN)
                    {
                        estimateORfraction *= (2.0 / 3.0);
                        relationOpMap[currCompOp->left->value] = currCompOp->code;
                    }
                    if (currCompOp->code == EQUALS)
                    {
                        estimateORfraction *= (1.0 - (1.0 / (*attributeDataStore)[leftRelation][currCompOp->left->value]));
                        relationOpMap[currCompOp->left->value] = currCompOp->code;
                    }
                }
            }

            orlist = orlist->rightOr;
        }
        if (!isdepth)
        {
            estimateORfraction = 1.0 - estimateORfraction;
        }

        isdepth = false;
        done = false;
        estimateANDfraction *= estimateORfraction;
        andlist = andlist->rightAnd;
    }

    if (isjoinInvolved == true)
    {
        // case of join involved in entire CNF used for estimate calculation
        double righttupleCount = (*relationDataStore)[joinRightRelation];
        double leftupleCount = (*relationDataStore)[joinLeftRelation];
        estimate = leftupleCount * righttupleCount * estimateANDfraction;
    }
    else
    {
        // case join no involved in the entire CNF
        double lefttuplecount = (*relationDataStore)[leftRelation];
        estimate = lefttuplecount * estimateANDfraction;
    }

    if (isApply)
    {
        // case where Apply is called, we adding the estimated values with proper relations to hash structures
        set<string> joinAttrSet;
        if (isjoinInvolved)
        {
            // case where join is involved in the CNF
            for (string_to_int_mapper::iterator relOpMapitr = relationOpMap.begin(); relOpMapitr != relationOpMap.end(); relOpMapitr++)
            {
                // looping over the relation data to find the relation 
                for (int i = 0; i < relationDataStore->size(); i++)
                {
                    if (relNames[i] == NULL)
                        continue;

                    int count = ((*attributeDataStore)[relNames[i]]).count(relOpMapitr->first);

                    if (count == 0)
                        continue;

                    else if (count == 1)
                    {

                        for (string_to_int_mapper::iterator distinctCountMapitr = (*attributeDataStore)[relNames[i]].begin(); distinctCountMapitr != (*attributeDataStore)[relNames[i]].end(); distinctCountMapitr++)
                        {

                            if ((relOpMapitr->second == LESS_THAN) || (relOpMapitr->second == GREATER_THAN))
                            {
                                (*attributeDataStore)[joinLeftRelation + "_" + joinRightRelation][distinctCountMapitr->first] = (int)round((double)(distinctCountMapitr->second) / 3.0);
                            }
                            else if (relOpMapitr->second == EQUALS)
                            {

                                if (relOpMapitr->first == distinctCountMapitr->first)
                                {
                                    (*attributeDataStore)[joinLeftRelation + "_" + joinRightRelation][distinctCountMapitr->first] = 1;
                                }
                                else
                                {
                                    (*attributeDataStore)[joinLeftRelation + "_" + joinRightRelation][distinctCountMapitr->first] = min((int)round(estimate), distinctCountMapitr->second);
                                }
                            }
                        }
                        break;
                    }
                    else if (count > 1)
                    {

                        for (string_to_int_mapper::iterator distinctCountMapitr = (*attributeDataStore)[relNames[i]].begin(); distinctCountMapitr != (*attributeDataStore)[relNames[i]].end(); distinctCountMapitr++)
                        {
                            if (relOpMapitr->second == EQUALS)
                            {
                                if (relOpMapitr->first == distinctCountMapitr->first)
                                {
                                    (*attributeDataStore)[joinLeftRelation + "_" + joinRightRelation][distinctCountMapitr->first] = count;
                                }
                                else
                                {
                                    (*attributeDataStore)[joinLeftRelation + "_" + joinRightRelation][distinctCountMapitr->first] = min((int)round(estimate), distinctCountMapitr->second);
                                }
                            }
                        }

                        break;
                    }
                    joinAttrSet.insert(relNames[i]);
                }
            }

            if (joinAttrSet.count(joinLeftRelation) == 0)
            {
                //case where only joins are involved, checking its exisiting and adding to hash structure
                for (string_to_int_mapper::iterator entry = (*attributeDataStore)[joinLeftRelation].begin(); entry != (*attributeDataStore)[joinLeftRelation].end(); entry++)
                {
                    (*attributeDataStore)[joinLeftRelation + "_" + joinRightRelation][entry->first] = entry->second;
                }
            }

            if (joinAttrSet.count(joinRightRelation) == 0)
            {
                // case where only joins are involved, checking its existing and adding to hash structure
                for (string_to_int_mapper::iterator entry = (*attributeDataStore)[joinRightRelation].begin(); entry != (*attributeDataStore)[joinRightRelation].end(); entry++)
                {
                    (*attributeDataStore)[joinLeftRelation + "_" + joinRightRelation][entry->first] = entry->second;
                }
            }
            // adding the estimate to relation hash structure
            (*relationDataStore)[joinLeftRelation + "_" + joinRightRelation] = round(estimate);
            relationDataStore->erase(joinLeftRelation);
            relationDataStore->erase(joinRightRelation);
            attributeDataStore->erase(joinLeftRelation);
            attributeDataStore->erase(joinRightRelation);
        }
        else
        {
            // case where join is not involved in the CNF
            relationDataStore->erase(leftRelation);
            relationDataStore->insert(make_pair(leftRelation, round(estimate)));
        }
    }
    cout << "estimated cost is " << estimate << endl;
    return estimate;
}

Statistics::~Statistics()
{
    // destructor for the statistics 
    delete relationDataStore;
    delete attributeDataStore;
}