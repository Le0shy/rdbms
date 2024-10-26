#include "../rbf/rbfm.h"
#include "../rm/rm.h"
#include "../ix/ix.h"
#include "qe.h"


//FILTER INTERFACE - where clause
Filter::Filter(Iterator* input, const Condition &condition) {
    //constructor method
    //assign input parameters to local instances 
    this->Iteratorinput=input;
    this->queryCondition=condition;
};

RC Filter::getNextTuple(void *data){
    //coped from intiial qe.class - tablescan section to start
    RC rc;
    Iteratorinput->getNextTuple(data); 
    if(rc != 0) return rc;
    vector<Attribute> attrbiteVector;
    getAttributes(attributeVector);

/*
    if(queryCondition.bRhsIsAttr == true) {
        if(queryCondition.rhsAttr == queryCondition.lhsAttr){
            //save tuple?
            int tupleLength = queryCondition.rhsAttr.size();
            void *data = malloc(tupleLength);
            return 0;
        }
        return rc;
    }
*/
    return 0;
};

void Filter::getAttributes(vector<Attribute> &attrs) const{ 
    //&attrs is the memory address
    //get Attributes from the Iterator Class
    //coped from intiial qe.class - tablescan section

    attrs.clear();
    attrs = this->attributeVector;
    unsigned i;

    // For attribute in vector<Attribute>, name it as rel.attr
    for(i = 0; i < attrs.size(); ++i)
    {
        string tmp = "";
        tmp += ".";
        tmp += attrs.at(i).name;
        attrs.at(i).name = tmp;
    }
};

//PROJECT INTERFACE - select clause
Project::Project(Iterator *input, const vector<string> &attrNames){
    Iteratorinput = input;
    
    // Get the attributes from the input iterator
    vector<Attribute> inputAttrs;
    Iteratorinput->getAttributes(inputAttrs);
    for (const string& name : attrNames) {
        for (const Attribute& attr : inputAttrs) {
            if (attr.name == name) {
                attributeVector.push_back(attr);
                break;
            }
        }
    }
};  

RC Project::getNextTuple(void *data) {
    RC rc;
    int bufferSize = 100;


    void* originalData = malloc(bufferSize); 
    while (Iteratorinput->getNextTuple(originalData) != QE_EOF) {
        
        char* readPtr = (char*)originalData;
        char* writePtr = (char*)data;

        // Iterate through the attribute vector and copy the required attributes
        for (const Attribute& attr : attributeVector) {
            // Check if the attribute is in the projection list

            }
            // Move the read pointer forward by the size of the attribute
    }
    free(originalData);
    return 0; 
};

// For attribute in vector<Attribute>, name it as rel.attr
void Project::getAttributes(vector<Attribute> &attrs) const {
    //&attrs is the memory address
    //get Attributes from the Iterator Class
    //coped from intiial qe.class - tablescan section

    attrs.clear();
    attrs = this->attributeVector;
    unsigned i;

    // For attribute in vector<Attribute>, name it as rel.attr
    for(i = 0; i < attrs.size(); ++i)
    {
        string tmp = "";
        tmp += ".";
        tmp += attrs.at(i).name;
        attrs.at(i).name = tmp;
    }
};


/*
//BNLJOIN INTERFACE
        BNLJoin(Iterator *leftIn,            // Iterator of input R
               TableScan *rightIn,           // TableScan Iterator of input S
               const Condition &condition,   // Join condition
               const unsigned numPages       // # of pages that can be loaded into memory,
        ){};

RC BNLJoin::getNextTuple(void *data){return QE_EOF;};

// For attribute in vector<Attribute>, name it as rel.attr
void BNLJoin::getAttributes(vector<Attribute> &attrs) const{};
*/


//AGGREGATE INTERFACE
Aggregate::Aggregate(Iterator *input, Attribute aggAttr, AggregateOp op){
};

RC Aggregate::getNextTuple(void *data){return QE_EOF;};

void Aggregate::getAttributes(vector<Attribute> &attrs) {


};

string aggregateOpToString(AggregateOp op) {
    switch (op) {
    case MIN: return "MIN";
    case MAX: return "MAX";
    case SUM: return "SUM";
    case AVG: return "AVG";
    case COUNT: return "COUNT";
    default: return "UNKNOWN";
    }
}