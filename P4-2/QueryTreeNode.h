#ifndef QUERY_TREE_NODE_H
#define QUERY_TREE_NODE_H

#include <iostream>
#include "Schema.h"
#include "DBFile.h"
#include "Heap.h"
#include "SortedFile.h"
#include "RelOp.h"

using namespace std;

enum QueryTreeNodeType {SELECTFILE, SELECTPIPE, PROJECT, JOIN, SUM, GROUPBY, DISTINCT, WRITEOUT};

class QueryTreeNode {

public:

    int numberOfAttributesIn, numberOfAttributesOut;
    int *attributesToKeep, numberOfAttributesToKeep;

    AndList *cnf;
    CNF *operatorForCNF;

    Schema *schema;

    OrderMaker *oMaker;

    FuncOperator *functionOperator;

    Function *function;

    string filePath;


    SelectFile *sf;
    SelectPipe *sp;
    Project *p;
    Join *j;
    Sum *s;
    GroupBy *gb;
    DuplicateRemoval *dr;
    WriteOut *wo;

    DBFile *db;

    QueryTreeNodeType nodeType;
    QueryTreeNode *parentNode, *leftNode, *rightNode;

    int leftChildPipeID, rightChildPipeID, outputPipeID;
    Pipe *leftInputPipe, *rightInputPipe, *outputPipe;

    QueryTreeNode();
    ~QueryTreeNode();

    void printNode();
    void printCNF();
    void printFunction();
    void printInOrder();
    void setQueryTreeNodeType(QueryTreeNodeType type);

    void Run();
    void WaitUntilDone();

    string getTypeName();
    QueryTreeNodeType getQueryTreeNodeType();

    void generateFunction();
    void generateSchema();
    void generateOrderMaker(int numberOfAttributes, vector<int> attributes, vector<int> types);

};

#endif