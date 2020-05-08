#ifndef RELATION_STATISTICS_H
#define RELATION_STATISTICS_H

#include <iostream>
#include <map>
#include <set>
#include <vector>
#include <sstream>
using namespace std;

class RelationStatistics {

    double numberOfRows;
    map<string, int> attributes;

public:

    RelationStatistics();
    RelationStatistics(double numOfRows);

    ~RelationStatistics();

    void updateNumberOfRowsCount(double numOfRows);

    void addAttribute(string attribute, int distinctVal);

    double getNumberOfRows();

    friend ostream& operator<< (ostream& os, RelationStatistics &relStats);
    friend istream& operator>> (istream& is, RelationStatistics &relStats);

    int operator[] (string attribute);

    RelationStatistics& operator= (RelationStatistics &relStat);


};

class RelationStatSet {
    
    double numberOfTuples;
    set<string> joinedSet;

public:

    RelationStatSet();
    RelationStatSet(string rel);

    ~RelationStatSet();

    void addRelationToSet(string rel);

    int intersect(RelationStatSet rss); // not a valid intersection = -1; rss has no intersection = 0; if intersection  = >0 value;

    int size();

    void merge(RelationStatSet rss);
    
    void updateNumberOfTuples(double numOfTuples);

    double getNumberOfTuples();

    void getRelations(vector<string> &sets);

    void printRelations();

    friend ostream& operator<< (ostream& os, RelationStatSet &set);
    friend istream& operator>> (istream& is, RelationStatSet &set);

    RelationStatSet& operator= (RelationStatSet &rss);

};

#endif