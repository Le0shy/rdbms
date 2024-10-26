#ifndef _pfm_h_
#define _pfm_h_

typedef unsigned PageNum;
//typedef int RC;


//#define PAGE_SIZE 4096
#include <string>
#include <climits>
#include <fstream>
#include <iostream>
#include <cstring>
#include <memory>
#include "config.h"
#include "dm.h"
#include "page.h"
#include "bpm.h"

using namespace std;

class FileHandle;

class PagedFileManager
{
public:
    static PagedFileManager* instance();                                  // Access to the _pf_manager instance
    static void deleteInstance();

    RC createFile(const string& fileName);                            // Create a new file
    RC destroyFile(const string& fileName);                            // Destroy a file
    RC openFile(const string& fileName, FileHandle& fileHandle);    // Open a file
    RC closeFile(FileHandle& fileHandle);                            // Close a file

protected:
    PagedFileManager();                                                   // Constructor
    ~PagedFileManager();                                                  // Destructor

private:
    static bool isFileExist(const string& fileName);                            // Return true if file exists
    auto createHiddenPage(std::ofstream& out) ->RC;
    static PagedFileManager* _pf_manager;

};


class FileHandle
{
friend class PagedFileManager;
friend class RecordBasedFileManager;
public:
    // Variables to keep the counter for each operation
    unsigned readPageCounter;
    unsigned writePageCounter;
    unsigned appendPageCounter;
    unsigned pagesNumber;
    unsigned recordCounter;

    FileHandle();                                                         // Default constructor
    ~FileHandle();                                                        // Destructor
    explicit FileHandle(BufferPoolManager* buffer_pool_manager);
    FileHandle& operator=(const FileHandle& fileHandle);

    void Initialize(const string & fileName);
    void Free();

    RC readPage(PageNum pageNum, void* data);                             // Get a specific page
    RC writePage(PageNum pageNum, const void* data);                      // Write a specific page
    RC appendPage(const void* data);                                      // Append a specific page

    RC collectCounterValues(unsigned& readPageCount,
                            unsigned& writePageCount, unsigned& appendPageCount);  // Put the current counter values into variables

    unsigned getNumberOfPages();                      // Get the number of pages in the file

    Page* Fetch(const page_id_t& page_id){
        return bp_manager_ -> FetchPage(page_id);
    }

    Page* FetchD(const uint16_t& record_length){
        return bp_manager_ -> FetchPageD(record_length);
    }

    Page* New(page_id_t &page_id) {
        return bp_manager_ ->NewPage(&page_id);
    }
    /* file io stream */
    std::fstream file_io_;

protected:
    std::string file_name_;
    BufferPoolManager *bp_manager_;

private:
    /* use is_free_ to indicate if a FileHandle Obj is allocated */
    bool is_free_;
    auto getNumberOfPagesFromDisk() -> RC;
    auto getReadPageCounter() -> RC;
    auto getWritePageCounter() -> RC;
    auto getAppendPageCounter() -> RC;
    auto getRecordCounter() -> RC;

    auto updateReadPageCounter() -> RC;
    auto updateWritePageCounter() -> RC;
    auto updateAppendPageCounter() -> RC;
    auto updatePagesNumber() ->RC;
    auto updateRecordCounter() ->RC;
};

#endif
