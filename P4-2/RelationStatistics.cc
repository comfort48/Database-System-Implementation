#ifndef RELATION_STATISTICS_CC
#define RELATION_STATISTICS_CC

#include "RelationStatistics.h"

RelationStatistics::RelationStatistics() {
    
    numberOfRows = 0;

}

RelationStatistics::RelationStatistics(double numOfRows) {

    numberOfRows = numOfRows;

}

void RelationStatistics::updateNumberOfRowsCount(double numOfRows) {

    numberOfRows = numOfRows;

}

void RelationStatistics::addAttribute(string attribute, int distinctCount) {

    attributes[attribute] = distinctCount;

}

double RelationStatistics::getNumberOfRows() {

    return numberOfRows;

}

ostream& operator<< (ostream& os, RelationStatistics &relStats) {

    os<<relStats.numberOfRows<<endl;
    os<<relStats.attributes.size()<<endl;

    for(map<string, int>::iterator it = relStats.attributes.begin(); it != relStats.attributes.end(); it++)
        os<<it->first<<endl<<it->second<<endl;
    
    return os;

}

istream& operator>> (istream& is, RelationStatistics &relStats) {

    int numberOfIterations = 0;
    string dummyLine;
    stringstream ss("");

    getline(is, dummyLine);
    ss.str(dummyLine);

    if(!(ss>>relStats.numberOfRows))
        relStats.numberOfRows = 0;
    
    ss.clear();
    ss.str("");

    getline(is, dummyLine);
    ss.str(dummyLine);

    if(!(ss>>numberOfIterations))
        numberOfIterations = 0;
    
    ss.clear();
    ss.str("");

    string attributeName;
    int distinctCount = 0;

    for(int i = 0; i < numberOfIterations; i++) {
        
        getline(is, dummyLine);
        attributeName = dummyLine;
        getline(is, dummyLine);
        ss.str(dummyLine);

        if(!(ss>>distinctCount)) 
            distinctCount = 0;
        
        ss.clear();
        ss.str("");

        relStats.attributes[attributeName] = distinctCount;

    }
    return is;

}

int RelationStatistics::operator[] (string attribute) {

    if(attributes.find(attribute) == attributes.end())
        return -2;
    
    else if(attributes[attribute] == -1)
        return numberOfRows;
    
    else
        return attributes[attribute];

}

RelationStatistics& RelationStatistics::operator= (RelationStatistics &relStats) {

    if(&relStats != this) {
        this->numberOfRows = relStats.numberOfRows;
        this->attributes.clear();
        this->attributes = relStats.attributes;
    }
    return *this;
}

RelationStatistics::~RelationStatistics() {

    attributes.clear();

}

// RelationStatSet class methods from here

RelationStatSet::RelationStatSet() {
    
    numberOfTuples = -1.0;

}

RelationStatSet::RelationStatSet(string rel) {
    
    joinedSet.insert(rel);
    numberOfTuples = -1.0;

}

void RelationStatSet::addRelationToSet(string rel) {
    joinedSet.insert(rel);
}

int RelationStatSet::intersect(RelationStatSet relStatSet) {

    RelationStatSet volatileRSS;

    for(set<string>::iterator it = relStatSet.joinedSet.begin(); it != relStatSet.joinedSet.end(); it++) {
        if(this->joinedSet.find(*it) != this->joinedSet.end()) {
            volatileRSS.addRelationToSet(*it);
        }
    }

    int res = volatileRSS.size();

    if(res == 0 || res == size())
        return res;
    else
        return -1;

}

int RelationStatSet::size() {
    
    return joinedSet.size();

}

void RelationStatSet::merge(RelationStatSet relStatSet) {

    for(set<string>::iterator it = relStatSet.joinedSet.begin(); it != relStatSet.joinedSet.end(); it++)
        addRelationToSet(*it);
    
}

void RelationStatSet::updateNumberOfTuples(double numTuples) {

    numberOfTuples = numTuples;

}

double RelationStatSet::getNumberOfTuples() {
    
    return numberOfTuples;

}

void RelationStatSet::getRelations(vector<string> &setsVec) {

    for(set<string>::iterator it = joinedSet.begin(); it != joinedSet.end(); it++) 
        setsVec.push_back(*it);

}

void RelationStatSet::printRelations() {

    for(set<string>::iterator it = joinedSet.begin(); it != joinedSet.end(); it++)
        cout<<*it<<endl;

}

ostream& operator<< (ostream& os, RelationStatSet &relStatSet) {

    os<<relStatSet.numberOfTuples<<endl;
    os<<relStatSet.joinedSet.size()<<endl;

    for(set<string>::iterator it = relStatSet.joinedSet.begin(); it != relStatSet.joinedSet.end(); it++)
        os<<*it<<endl;
    
    return os;

}

istream& operator>> (istream& is, RelationStatSet &relStatSet) {

    string dummyLine;
    stringstream ss;

    double numberOfTuples = 0;
    int numberOfRelations = 0;

    getline(is, dummyLine);
    ss.str(dummyLine);

    if(!(ss>>numberOfTuples))
        numberOfTuples = 0.0;
    
    ss.str("");
    ss.clear();

    relStatSet.updateNumberOfTuples(numberOfTuples);

    getline(is, dummyLine);
    ss.str(dummyLine);

    if(!(ss>>numberOfRelations))
        numberOfRelations = 0;
    
    ss.str("");
    ss.clear();

    for(int i = 0; i < numberOfRelations; i++) {
        getline(is, dummyLine);
        relStatSet.joinedSet.insert(dummyLine);
    }

    return is;

}

RelationStatSet& RelationStatSet::operator= (RelationStatSet &rss) {

    if(&rss != this) {
        this->numberOfTuples = rss.numberOfTuples;
        this->joinedSet = rss.joinedSet;
    }

    return *this;
}

RelationStatSet::~RelationStatSet(){}

#endif