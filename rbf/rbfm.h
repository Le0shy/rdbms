#ifndef _rbfm_h_
#define _rbfm_h_

#include <string>
#include <vector>
#include <climits>
#include <functional>

#include "config.h"
#include "pfm.h"
#include "page.h"
#include "record.h"


using namespace std;

/********************************************************************************
The scan iterator is NOT required to be implemented for the part 1 of the project 
********************************************************************************/

# define RBFM_EOF (-1)  // end of a scan operator

// RBFM_ScanIterator is an iterator to go through records
// The way to use it is like the following:
//  RBFM_ScanIterator rbfmScanIterator;
//  rbfm.open(..., rbfmScanIterator);
//  while (rbfmScanIterator(rid, data) != RBFM_EOF) {
//    process the data;
//  }
//  rbfmScanIterator.close();

class RBFM_ScanIterator {
public:
  RBFM_ScanIterator() {};
  ~RBFM_ScanIterator() {};

    void Initialize(FileHandle* filehandle, const vector<Attribute> &rd, const string &conditionAttr,
                               const CompOp comOp, void *value, const vector<string> &attriNames){
        /* Init FileHande, record descriptor, comparator, condition value, attribute names to be projected. */
        this->file_handle_ = filehandle;
        this->rd_ = rd;
        this->com_op_ = comOp;
        this->value_ = value;
        this->attr_names_ = attriNames;
        this->record_counter_ = 0;
        this->page_counter_ = 2;
        this->next_slot_id_ = 0;
        /* get record number and page number from 'filehandle' */
        this->record_number_ = filehandle ->recordCounter;
        this->page_number_ = filehandle ->pagesNumber + 2;

        /* get attribute to which predication is used */
        for(size_t i = 0; i < rd.size(); i += 1){
            if(conditionAttr == rd_[i].name){
                this -> condition_attr_ = rd_[i];
                this -> condition_attr_pos_ = i;
                break;
            }
        }

        this -> page_cur_ = static_cast<RecordPage*>(file_handle_ ->Fetch(page_counter_));

        while(!page_cur_ ->ReadFirstRecordRid(&cur_slot_id_)){
            page_counter_ += 1;
            if(page_counter_ < page_number_) {
                page_cur_ = static_cast<RecordPage *>(file_handle_->Fetch(page_counter_));
            }
        }

        this -> rid_.pageNum = page_counter_;
        this -> rid_.slotNum = cur_slot_id_;
    }

  // Never keep the results in the memory. When getNextRecord() is called,
  // a satisfying record needs to be fetched from the file.
  // "data" follows the same format as RecordBasedFileManager::insertRecord().
  RC getNextRecord(RID &rid, void *data);
  RC close();

  RC UpdateIterator();
  RC GetNextPage();
  RC FilterRecord();
  template<typename T> bool Compare(T attribute, T value, CompOp op) const;
  RC ProjRecord(void* data);

protected:
    FileHandle* file_handle_;
    vector<Attribute> rd_;
    Attribute condition_attr_;
    size_t condition_attr_pos_;
    CompOp com_op_;
    /* how could it be const ? */
    void* value_;
    vector<string> attr_names_;

    RID rid_{};
    uint32_t record_counter_ = 0;
    uint32_t record_number_ = 0;

    slot_id_t cur_slot_id_ = 0;
    slot_id_t next_slot_id_ = 0;

    page_id_t page_counter_ = 2;
    //TODO()
    page_id_t page_number_ = 0;

    Record record_;
    RecordPage* page_cur_ = nullptr;
};


//Keep in mind - 0 indicates normal completion 
//Nonzero code indiciates an exception or error has occured
//the following do not need to be implemented: deleteRecord, updateRecord, readAttribute, and scan

class RecordBasedFileManager
{
public:
  // Access to the _rbf_manager instance
  static RecordBasedFileManager* instance();

  // deconstruct _rbf_manager instance
  static void deleteInstance();

  // Create a new record-based file
  RC createFile(const string &fileName);
  
  // Destroy a record-based file
  RC destroyFile(const string &fileName);
  
  // Open a record-based file
  RC openFile(const string &fileName, FileHandle &fileHandle);
  
  // Close a record-based file
  RC closeFile(FileHandle &fileHandle);

  // Insert a record into a file
  //  Format of the data passed into the function is the following:
  //  [n byte-null-indicators for y fields] [actual value for the first field] [actual value for the second field] ...
  //  1) For y fields, there is n-byte-null-indicators in the beginning of each record.
  //     The value n can be calculated as: ceil(y / 8). (e.g., 5 fields => ceil(5 / 8) = 1. 12 fields => ceil(12 / 8) = 2.)
  //     Each bit represents whether each field value is null or not.
  //     If k-th bit from the left is set to 1, k-th field value is null. We do not include anything in the actual data part.
  //     If k-th bit from the left is set to 0, k-th field contains non-null values.
  //     If there are more than 8 fields, then you need to find the corresponding byte first, 
  //     then find a corresponding bit inside that byte.
  //  2) Actual data is a concatenation of values of the attributes.
  //  3) For Int and Real: use 4 bytes to store the value;
  //     For Varchar: use 4 bytes to store the length of characters, then store the actual characters.
  //  !!! The same format is used for updateRecord(), the returned data of readRecord(), and readAttribute().
  // For example, refer to the Q8 of Project 1 wiki page.
  RC insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid);

  // insertionHelper
  RC insertRecordHelperFunction(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const int &recordTotalLength, RID &rid);

  // Read a record identified by the given rid.
  RC readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data);
  
  // Print the record that is passed to this utlity method.
  // This method will be mainly used for debugging/testing. 
  // The format is as follows:
  // field1-name: field1-value  field2-name: field2-value ... \n
  // (e.g., age: 24  height: 6.1  salary: 9000
  //        age: NULL  height: 7.5  salary: 7500)
  RC printRecord(const vector<Attribute> &recordDescriptor, const void *data);

/******************************************************************************************************************************************************************
IMPORTANT, PLEASE READ: All methods below this comment (other than the constructor and destructor) are NOT required to be implemented for the part 1 of the project
******************************************************************************************************************************************************************/
  //DO NOT IMPLEMENT
  RC deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid);

  // Assume the RID does not change after an update
  //DO NOT IMPLEMENT
  RC updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid);

  //DO NOT IMPLEMENT
  RC readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data);

  // Scan returns an iterator to allow the caller to go through the results one by one. 
  //DO NOT IMPLEMENT
  /**
   * Given a record descriptor, scan a file, i.e., sequentially read all the entries in the file.
   * A scan has a filter condition associated with it, e.g., it consists of a list of attributes
   * to project out as well as a predicate on an attribute ("Sal > 40000").
   * Specifically, the parameter conditionAttribute here is the attribute's name that you are going
   * to apply the filter on. The compOp parameter is the comparison type that is going to
   * be used in the filtering process.
   * The value parameter is the value of the conditionAttribute that is going to be used to filter out records.
   * Note that the retrieved records should only have the fields that are listed in the vector attributeNames.
   * Please take a look at the test cases for more information on how to use this method.
   * @param fileHandle
   * @param recordDescriptor
   * @param conditionAttribute the attribute's name that you are going to apply the filter on.
   * @param compOp comparison type that is going to be used in the filtering process.
   * @param value the value of the conditionAttribute that is going to be used to filter out records.
   * @param attributeNames the retrieved records should only have the fields that are listed in the vector attributeNames.
   * @param rbfm_ScanIterator
   * @return execution state
   */
  RC scan(FileHandle &fileHandle,
      const vector<Attribute> &recordDescriptor,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparision type such as "<" and "="
      void *value,                          // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RBFM_ScanIterator &rbfm_ScanIterator);

public:
    static PagedFileManager * pf_manager_;
    //auto GetRecordNumber(FileHandle& file_handle, const string& file_name) -> size_t;

protected:
  RecordBasedFileManager();
  ~RecordBasedFileManager();

private:
  static RecordBasedFileManager *_rbf_manager;

};


#endif
