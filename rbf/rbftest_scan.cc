#include <iostream>
#include <string>
#include <cassert>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#include "pfm.h"
#include "rbfm.h"
#include "test_util.h"

using namespace std;

//void *record = malloc(2000);
//void *returnedData = malloc(2000);
//vector<Attribute> recordDescriptor;
//unsigned char *nullsIndicator = NULL;
//FileHandle fileHandle;
//
//void readRecord(RecordBasedFileManager *rbfm, const RID& rid, string str)
//{
//    int recordSize;
//    prepareRecord(recordDescriptor.size(), nullsIndicator, str.length(), str, 25, 177.8, 6200,
//                  record, &recordSize);
//
//    RC rc = rbfm->readRecord(fileHandle, recordDescriptor, rid, returnedData);
//    assert(rc == success && "Reading a record should not fail.");
//
//    // Compare whether the two memory blocks are the same
//    assert(memcmp(record, returnedData, recordSize) == 0 && "Returned Data should be the same");
//}
//
//void insertRecord(RecordBasedFileManager *rbfm, RID& rid, string str)
//{
//    int recordSize;
//    prepareRecord(recordDescriptor.size(), nullsIndicator, str.length(), str, 25, 177.8, 6200,
//                  record, &recordSize);
//
//    RC rc = rbfm->insertRecord(fileHandle, recordDescriptor, record, rid);
//    assert(rc == success && "Inserting a record should not fail.");
//
//}
//
//void updateRecord(RecordBasedFileManager *rbfm, RID& rid, string str)
//{
//    int recordSize;
//    prepareRecord(recordDescriptor.size(), nullsIndicator, str.length(), str, 25, 177.8, 6200,
//                  record, &recordSize);
//
//    RC rc = rbfm->updateRecord(fileHandle, recordDescriptor, record, rid);
//    assert(rc == success && "Updating a record should not fail.");
//
//}



int RBFTest_Scan(RecordBasedFileManager *rbfm)
{
    // Functions tested
    // 1. Create Record-Based File
    // 2. Open Record-Based File
    // 3. Insert Record
    // 4. Read Record
    // 5. Close Record-Based File
    // 6. Destroy Record-Based File
    cout << endl << "***** In RBF Test Case Scan *****" << endl;

    RC rc;
    string fileName = "test_scan";
    FileHandle fileHandle;
    vector<Attribute> attrs;
    // Create a file
    rc = rbfm->createFile(fileName);
    assert(rc == success && "Creating the file should not fail.");

    rc = createFileShouldSucceed(fileName);
    assert(rc == success && "Creating the file should not fail.");

    // Open the file
    rc = rbfm->openFile(fileName, fileHandle);
    assert(rc == success && "Opening the file should not fail.");
    RID rid;
    void *record = malloc(1000);
    int numRecords = 2000;
    int recordSize = 0;
    void *returnedData = malloc(200);

    int ageVal = 25;
    int age = 0;
    RID rids[numRecords];
    vector<char *> tuples;
    string tupleName;
    char *suffix = (char *)malloc(10);
    bool nullBit = false;
    createRecordDescriptor(attrs);

    int nullAttributesIndicatorActualSize = getActualByteForNullsIndicator(attrs.size());
    unsigned char *nullsIndicator = (unsigned char *) malloc(nullAttributesIndicatorActualSize);
    memset(nullsIndicator, 0, nullAttributesIndicatorActualSize);

    unsigned char *nullsIndicatorWithNull = (unsigned char *) malloc(nullAttributesIndicatorActualSize);
    memset(nullsIndicatorWithNull, 0, nullAttributesIndicatorActualSize);
    // age field : NULL
    nullsIndicatorWithNull[0] = 64; // 01000000

    for(int i = 0; i < numRecords; i++)
    {
        float height = (float)i;

        age = (rand()%20) + 15;

        sprintf(suffix, "%d", i);

        if (i % 10 == 0) {
            tupleName = "TesterNull";
            tupleName += suffix;
            prepareRecord(attrs.size(), nullsIndicatorWithNull,
                          tupleName.length(), tupleName,0,
                          height, 456, record, &recordSize);
        } else {
            tupleName = "Tester";
            tupleName += suffix;
            prepareRecord(attrs.size(), nullsIndicator,
                          tupleName.length(),tupleName, age, height, 123,
                         record, &recordSize);
        }
        rc = rbfm->insertRecord(fileHandle, attrs, record, rid);
        assert(rc == success && "RBFM::insertRecord() should not fail.");

        rids[i] = rid;
    }

    // Set up the iterator
    RBFM_ScanIterator rmsi;
    string attr = "Age";
    vector<string> attributes;
    attributes.push_back(attr);
    rc = rbfm->scan(fileHandle, attrs, attr,GT_OP,
                    &ageVal, attributes, rmsi);
    assert(rc == success && "RelationManager::scan() should not fail.");

    while(rmsi.getNextRecord(rid, returnedData) != RBFM_EOF)
    {
// Check the first bit of the returned data since we only return one attribute in this test case
// However, the age with NULL should not be returned since the condition NULL > 25 can't hold.
// All comparison operations with NULL should return FALSE
// (e.g., NULL > 25, NULL >= 25, NULL <= 25, NULL < 25, NULL == 25, NULL != 25: ALL FALSE)
        nullBit = *(unsigned char *)((char *)returnedData) & (1 << 7);
        if (!nullBit) {
            age = *(int *)((char *)returnedData+1);
            if (age <= ageVal) {
// Comparison didn't work in this case
                cout << "Returned value from a scan is not correct: returned Age <= 25." << endl;
                cout << "***** [FAIL] Test Case 13B Failed *****" << endl << endl;
                rmsi.close();
                free(returnedData);
                free(suffix);
                free(nullsIndicator);
                free(nullsIndicatorWithNull);
                return -1;
            }
        } else {
// Age with NULL value should not be returned.
            cout << "Returned value from a scan is not correct. NULL returned." << endl;
            cout << "***** [FAIL] Test Case 13B Failed *****" << endl << endl;
            rmsi.close();
            free(returnedData);
            free(suffix);
            free(nullsIndicator);
            free(nullsIndicatorWithNull);
            return -1;
        }
    }
    rmsi.close();
    free(record);
    free(returnedData);
    free(suffix);
    free(nullsIndicator);
    free(nullsIndicatorWithNull);

    rbfm ->closeFile(fileHandle);
    rbfm ->destroyFile(fileName);

    cout << "Test Case 13B Finished. The result will be examined. *****" << endl << endl;

    return success;
}

int main()
{
// To test the functionality of the record-based file manager
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    remove("test_update");

    RC rcmain = RBFTest_Scan(rbfm);
    rbfm ->deleteInstance();
    return rcmain;
}
