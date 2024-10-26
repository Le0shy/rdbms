#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>

#include "../rbf/rbfm.h"

using namespace std;

# define RM_EOF (-1)  // end of a scan operator

// RM_ScanIterator is an iterator to go through tuples
class RM_ScanIterator {
public:
    RM_ScanIterator(){} ;
    ~RM_ScanIterator(){} ;

    // "data" follows the same format as RelationManager::insertTuple()
    RC getNextTuple(RID &rid, void *data) {
        return rbfm_siterator_.getNextRecord(rid, data);
    };
    RC close() {
        rbfm_siterator_.close();
        rbfm_ ->closeFile(file_handle_);
        return 0;
    };

    void Initialize(RecordBasedFileManager* rbfm, const string tabeleName, const vector<Attribute> &rd,
                    const string &conditionAttr, const CompOp comOp, void *value, const vector<string> &attriNames){
        rbfm_ = rbfm;
        rbfm_ ->openFile(tabeleName, file_handle_);
        rbfm_siterator_.Initialize(&file_handle_, rd, conditionAttr, comOp, value, attriNames);
    }

private:
    RecordBasedFileManager *rbfm_;
    FileHandle file_handle_;
    RBFM_ScanIterator rbfm_siterator_;
};

/*
IGNORE - DO NOT IMPLEMENT
Added to remove current errors given off by QE class
*/
class RM_IndexScanIterator {
 public:
  RM_IndexScanIterator();  	// Constructor
  ~RM_IndexScanIterator(); 	// Destructor

  // "key" follows the same format as in IndexManager::insertEntry()
  RC getNextEntry(RID &rid, void *key);  	// Get next matching entry
  RC close();             			// Terminate index scan
};


// Relation Manager
class RelationManager
{
public:
  static RelationManager* instance();

  RC createCatalog();

  RC deleteCatalog();

  RC createTable(const string &tableName, const vector<Attribute> &attrs);

  RC deleteTable(const string &tableName);

  RC getAttributes(const string &tableName, vector<Attribute> &attrs);

  RC insertTuple(const string &tableName, const void *data, RID &rid);

  RC deleteTuple(const string &tableName, const RID &rid);

  RC updateTuple(const string &tableName, const void *data, const RID &rid);

  RC readTuple(const string &tableName, const RID &rid, void *data);

  // Print a tuple that is passed to this utility method.
  // The format is the same as printRecord().
  RC printTuple(const vector<Attribute> &attrs, const void *data);

  RC readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data);

  /*
  IGNORE - DO NOT IMPLEMENT - functions related to IndexManager - added to fix error in QE class
  */
  RC createIndex(const string &tableName, const string &attributeName);

  RC destroyIndex(const string &tableName, const string &attributeName);

  RC indexScan(const string &tableName, const string &attributeName, const void *lowKey, const void *highKey, bool lowKeyInclusive, bool highKeyInclusive, RM_IndexScanIterator &rm_IndexScanIterator);

  // Scan returns an iterator to allow the caller to go through the results one by one.
  // Do not store entire results in the scan iterator.
  RC scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparison type such as "<" and "="
      void *value,                          // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RM_ScanIterator &rm_ScanIterator);

// Extra credit work (10 points)
public:
  RC addAttribute(const string &tableName, const Attribute &attr);

  RC dropAttribute(const string &tableName, const string &attributeName);

private:
  void prepareTablesRecord(int fieldCount, unsigned char* nullFieldsIndicator, 
      const int tableId, const string& tableName, const string& fileName, 
      void* buffer, int bufferSize);

  void prepareColumnsRecord(int fieldCount, unsigned char* nullFieldsIndicator,
      const int tableId, const string& columnName, const int columnType, 
      const int columnLength, const int columnPosition, 
      void* buffer, int bufferSize);

protected:
    RecordBasedFileManager *rbfm_;
  RelationManager();
  ~RelationManager();

};

#endif
