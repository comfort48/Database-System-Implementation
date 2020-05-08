#ifndef QUERY_TREE_NODE_CC
#define QUERY_TREE_NODE_CC

#include "QueryTreeNode.h"

QueryTreeNode::QueryTreeNode() {

    
    attributesToKeep = NULL;
    numberOfAttributesToKeep = NULL;
    
    cnf = NULL;
    operatorForCNF = NULL;

    schema = NULL;

    oMaker = NULL;

    functionOperator = NULL;

    function = NULL;

    sf = NULL;
    sp = NULL;
    p = NULL;
    j = NULL;
    s = NULL;
    gb = NULL;
    dr = NULL;
    wo = NULL;

    db = NULL;

    parentNode = NULL;
    leftNode = NULL;
    rightNode = NULL;

    leftChildPipeID = 0;
    rightChildPipeID = 0;
    outputPipeID = 0;

}

void QueryTreeNode::printNode() {

    cout<<" *********** "<<endl;
    cout<< getTypeName()<<" operation"<<endl;

    switch (nodeType) {

        case SELECTFILE:

            cout<<"Input Pipe "<<leftChildPipeID<<endl;
            cout<<"Output Pipe "<<outputPipeID<<endl;
            cout<<"Output Schema: "<<endl;
            schema->Print();
            printCNF();
            break;
        
        case SELECTPIPE:

            cout<<"Input Pipe "<<leftChildPipeID<<endl;
            cout<<"Output Pipe "<<outputPipeID<<endl;
            cout<<"Output Schema: "<<endl;
            schema->Print();
            cout<<"SELECTION CNF :"<<endl;
            printCNF();
            break;
        
        case PROJECT:

            cout<<"Input Pipe "<<leftChildPipeID<<endl;
            cout<<"Output Pipe "<<outputPipeID<<endl;
            cout<<"Output Schema: "<<endl;
            schema->Print();
            cout<<endl<<"************"<<endl;
            break;
        
        case JOIN:

            cout<<"Left Input Pipe "<<leftChildPipeID<<endl;
            cout<<"Right Input Pipe "<<rightChildPipeID<<endl;
            cout<<"Output Pipe "<<outputPipeID<<endl;
            cout<<"Output Schema: "<<endl;
            schema->Print();
            cout<<endl<<"CNF: "<<endl;
            printCNF();
            break;
        
        case SUM:

            cout<<"Left Input Pipe "<<leftChildPipeID<<endl;
            cout<<"Output Pipe "<<outputPipeID<<endl;
            cout<<"Output Schema: "<<endl;
            schema->Print();
            cout<<endl<<"FUNCTION: "<<endl;
            printFunction();
            break;
        
        case DISTINCT:

            cout<<"Left Input Pipe "<<leftChildPipeID<<endl;
            cout<<"Output Pipe "<<outputPipeID<<endl;
            cout<<"Output Schema: "<<endl;
            schema->Print();
            // cout<<endl<<"FUNCTION: "<<endl;
            // printFunction();
            break;
        
        case GROUPBY:

            cout<<"Left Input Pipe "<<leftChildPipeID<<endl;
            cout<<"Output Pipe "<<outputPipeID<<endl;
            cout<<"Output Schema: "<<endl;
            schema->Print();
            cout<<endl<<"GROUPING ON: "<<endl;
            oMaker->Print();
            cout<<endl<<"FUNCTION: "<<endl;
            printFunction();
            break;
        
        case WRITEOUT:

            cout<<"Left Input Pipe "<<leftChildPipeID<<endl;
            cout<<"Output File "<<filePath<<endl;
            break;
    }

}

void QueryTreeNode::printCNF(){

    if(cnf) {

        struct AndList *currentAnd = cnf;

        struct OrList *currentOr;

        struct ComparisonOp *currentOperator;

        while(currentAnd) {

            currentOr = currentAnd->left;

            if(currentAnd->left) {
                cout<<"(";
            }
            while(currentOr) {
                currentOperator = currentOr->left;
                if(currentOperator) {
                    if(currentOperator->left) {
                        cout<<currentOperator->left->value;
                    }

                    switch(currentOperator->code) {
                        case 5:
                            cout<<" < ";
                            break;
                        case 6:
                            cout<<" > ";
                            break;
                        case 7:
                            cout<<" = ";
                            break;
                    }
                    if(currentOperator->right) {
                        cout<<currentOperator->right->value;
                    }
                }
                if(currentOr->rightOr) {
                    cout<<" OR ";
                }
                currentOr = currentOr->rightOr;
            }
            if(currentAnd->left) {
                cout<<")";
            }
            if(currentAnd->rightAnd) {
                cout<<" AND ";
            }
            currentAnd = currentAnd->rightAnd;
        }
    }
    cout<<endl;
}

void QueryTreeNode::printFunction(){

    function->Print();

}

void QueryTreeNode::printInOrder(){
    if(leftNode != NULL)
        leftNode->printInOrder();
    if(rightNode != NULL)
        rightNode->printInOrder();
    printNode();
}

void QueryTreeNode::setQueryTreeNodeType(QueryTreeNodeType type) {

    nodeType = type;

}

void QueryTreeNode::Run(){

    switch(nodeType) {
        
        case SELECTFILE:
        case SELECTPIPE:
        case PROJECT:
        case JOIN:
        case SUM:
        case GROUPBY:
        case DISTINCT:
        case WRITEOUT:
            break;

    }

}

void QueryTreeNode::WaitUntilDone() {

    switch(nodeType) {
        
        case SELECTFILE:
            sf->WaitUntilDone();
            break;
        case SELECTPIPE:
            sp->WaitUntilDone();
            break;
        case PROJECT:
        case JOIN:
        case SUM:
        case GROUPBY:
        case DISTINCT:
        case WRITEOUT:
            break;

    }

}

string QueryTreeNode::getTypeName() {
    string returnStr;

    switch(nodeType) {
        
        case SELECTFILE:
            returnStr = "SELECT FILE";
            break;
        case SELECTPIPE:
            returnStr = "SELECT PIPE";
            break;
        case PROJECT:
            returnStr = "PROJECT";
            break;
        case JOIN:
            returnStr = "JOIN";
            break;
        case SUM:
            returnStr = "SUM";
            break;
        case GROUPBY:
            returnStr = "GROUP BY";
            break;
        case DISTINCT:
            returnStr = "DISTINCT";
            break;
        case WRITEOUT:
            returnStr = "WRITE";
            break;  
        
    }
    return returnStr;

}

QueryTreeNodeType QueryTreeNode::getQueryTreeNodeType() {

    return nodeType;

}



void QueryTreeNode::generateFunction(){
    
    function = new Function();

    function->GrowFromParseTree(functionOperator, *schema);

}


void QueryTreeNode::generateSchema() {

    Schema *leftChildSchema = leftNode->schema;
    Schema *rightChildSchema = rightNode->schema;

    schema = new Schema(leftChildSchema, rightChildSchema);

}

void QueryTreeNode::generateOrderMaker(int numberOfAttributes, vector<int> attributes, vector<int> types) {

    oMaker = new OrderMaker();
    oMaker->numAtts = numberOfAttributes;

    for(int i = 0; i < attributes.size(); i++) {
        oMaker->whichAtts[i] = attributes[i];
        oMaker->whichTypes[i] = (Type)types[i];
    }
}

QueryTreeNode::~QueryTreeNode(){}

#endif