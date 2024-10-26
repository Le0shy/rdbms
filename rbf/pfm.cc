#include "pfm.h"
// To make this file clean, libraries are better included in the header file

PagedFileManager *PagedFileManager::_pf_manager = nullptr;

PagedFileManager *PagedFileManager::instance()
{
    if (!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}

void PagedFileManager::deleteInstance() {
    delete _pf_manager;
}

PagedFileManager::PagedFileManager()
{
}

PagedFileManager::~PagedFileManager()
{
}

bool PagedFileManager::isFileExist(const string &fileName)
{
    ifstream file(fileName);
    return file.good();
}

auto PagedFileManager::createHiddenPage(std::ofstream& out) ->RC
{
    Page* hidden_page = new Page();
    PageDirectory* page_directory = new PageDirectory();

    if (out.is_open())
    {
        out.write(
            hidden_page -> GetPage(),
            PAGE_SIZE);
        out.write(
                page_directory ->GetPage(),
                PAGE_SIZE);

        delete page_directory;
        delete hidden_page;
        return 0;
    }
    else
    {
        delete page_directory;
        delete hidden_page;
        return -1;
    }
}

RC PagedFileManager::createFile(const string &fileName)
{
    // Check if the file has already existed
    if (isFileExist(fileName))
    {
        cout << "File already exists: " << fileName << endl;
        return 1;
    }
    else
    {   
        /* Hidden Pages: Page for counter and other possible values
           and Page Directroy */
        ofstream write_paged_file(fileName);
        if (write_paged_file.is_open())
        {
            auto rc = createHiddenPage(write_paged_file);
            cout << "File is created successfully: " << fileName << endl;
            write_paged_file.close();
            return rc;
        }
        else
        {
            cerr << "Unable to create the file: " << fileName << endl;
            return 2;
        }
    }
}

RC PagedFileManager::destroyFile(const string &fileName)
{
    if (isFileExist(fileName))
    {
        const char *file_name = fileName.c_str();
        remove(file_name);
        return 0;
    }
    cout << "File does not exist: " << fileName << endl;
    return -1;
}

RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
    if (!isFileExist(fileName))
    {
        cout << "File does not exist: " << fileName << endl;
        return 1;
    }
    else
    {
        if (fileHandle.is_free_)
        {
            fileHandle.Initialize(fileName);
            fileHandle.getNumberOfPagesFromDisk();
            fileHandle.getReadPageCounter();
            fileHandle.getWritePageCounter();
            fileHandle.getAppendPageCounter();
            fileHandle.getRecordCounter();
            return 0;
        }
        else
        {
            cerr << "Unable to read the file: " << fileName << endl;
            return 2;
        }
    }
}

RC PagedFileManager::closeFile(FileHandle &fileHandle)
{ 
    fileHandle.updateReadPageCounter();
    fileHandle.updateWritePageCounter();
    fileHandle.updateAppendPageCounter();
    fileHandle.updatePagesNumber();
    fileHandle.updateRecordCounter();
    fileHandle.Free();
    return 0;
}

FileHandle::FileHandle()
{
    readPageCounter = 0;
    writePageCounter = 0;
    appendPageCounter = 0;

    is_free_ = true;
}

FileHandle::FileHandle(BufferPoolManager *bp_manager) : bp_manager_(bp_manager)
{
    readPageCounter = 0;
    writePageCounter = 0;
    appendPageCounter = 0;

    is_free_ = false;
}

/* It's a wrong implementation and won't be called! */
FileHandle &FileHandle::operator=(const FileHandle &other)
{
    if (this != &other)
    {
        bp_manager_ = other.bp_manager_;
        readPageCounter = other.readPageCounter;
        writePageCounter = other.writePageCounter;
        appendPageCounter = other.appendPageCounter;
        is_free_ = other.is_free_;
    }
    return *this;
}
FileHandle::~FileHandle()
{
    delete bp_manager_;
}

void FileHandle::Initialize(const string &fileName) 
{
    file_name_ = fileName;
    file_io_.open(file_name_, std::ios::binary | std::ios::in | std::ios::out);
    auto disk_manager = new DiskManager(fileName);
    auto bp_manager = new BufferPoolManager(disk_manager);
    bp_manager_ = bp_manager;
}

void FileHandle::Free()
{   
    is_free_ = true;
    file_io_.close();
    bp_manager_->FlushAll();
    delete bp_manager_;
    bp_manager_ = nullptr;
}

RC FileHandle::readPage(PageNum pageNum, void *data)
{
    if (pageNum + 1 > pagesNumber)
    {
        cout << "Page number does not exist" << endl;
        return -1;
    }

    pageNum = pageNum + 2;
    Page *page = bp_manager_->FetchPage(pageNum);
    char* page_block = page->GetPage();
    memcpy(data, page_block , PAGE_SIZE);

    readPageCounter += 1;
    return 0;
}

RC FileHandle::writePage(PageNum pageNum, const void *data)
{
     if (pageNum + 1 > pagesNumber)
     {
         std::cout << "Page number does not exist" << std::endl;
         return -1;
     }
    pageNum = pageNum + 2;
    Page *page = bp_manager_->FetchPage(pageNum);
    char *page_block = page->GetPage();
    memcpy(page_block, data, PAGE_SIZE);
    page -> SetDirty();

    writePageCounter += 1;
    return 0;
}

RC FileHandle::appendPage(const void *data)
{
    page_id_t new_page_id;
    Page *page = bp_manager_ -> NewPage(&new_page_id);
    char *page_block = page -> GetPage();
    memcpy(page_block, data, PAGE_SIZE);
    page -> SetDirty();

    pagesNumber += 1;
    appendPageCounter += 1;
    return 0;
}

unsigned FileHandle::getNumberOfPages() {
    return pagesNumber;
}

auto FileHandle::getNumberOfPagesFromDisk() -> RC
{
    if (file_io_.is_open())
    {
        file_io_.seekg(0);
        file_io_.read(reinterpret_cast<char *>(&pagesNumber), sizeof(pagesNumber));
        return 0;
    }
    else
    {
        return -1;
    }
}

auto FileHandle::getAppendPageCounter() -> RC
{
    if (file_io_.is_open())
    {
        file_io_.seekg(sizeof(appendPageCounter) * 3);
        file_io_.read(reinterpret_cast<char *>(&appendPageCounter), sizeof(appendPageCounter));
        return 0;
    }
    return -1;
}

auto FileHandle::getReadPageCounter() -> RC
{
    if (file_io_.is_open())
    {
        file_io_.seekg(sizeof(readPageCounter));
        file_io_.read(reinterpret_cast<char *>(&readPageCounter), sizeof(readPageCounter));
        return 0;
    }
    return -1;
}

auto FileHandle::getWritePageCounter() -> RC
{
    if (file_io_.is_open())
    {
        file_io_.seekg(sizeof(writePageCounter) * 2);
        file_io_.read(reinterpret_cast<char *>(&writePageCounter), sizeof(writePageCounter));
        return 0;
    }
    return -1;
}

auto FileHandle::getRecordCounter() -> RC {
    if(file_io_.is_open())
    {
        file_io_.seekg(sizeof(unsigned)*4);
        file_io_.read(reinterpret_cast<char *>(&recordCounter), sizeof(unsigned));
        return 0;
    }
    return -1;
}

RC FileHandle::updateAppendPageCounter()
{
    if (file_io_.is_open())
    {
        file_io_.seekp(sizeof(appendPageCounter) * 3);
        file_io_.write(reinterpret_cast<char *>(&appendPageCounter), sizeof(appendPageCounter));
        return 0;
    }
    return -1;
}

RC FileHandle::updateReadPageCounter()
{
    if (file_io_.is_open())
    {
        file_io_.seekp(sizeof(readPageCounter));
        file_io_.write(reinterpret_cast<char *>(&readPageCounter), sizeof(readPageCounter));
        return 0;
    }
    return -1;
}

RC FileHandle::updateWritePageCounter()
{
    if (file_io_.is_open())
    {
        file_io_.seekp(sizeof(writePageCounter) * 2);
        file_io_.write(reinterpret_cast<char *>(&writePageCounter), sizeof(writePageCounter));
        return 0;
    }
    return -1;
}

RC FileHandle::updatePagesNumber()
{
    if (file_io_.is_open())
    {
        file_io_.seekp(0);
        file_io_.write(reinterpret_cast<char *>(&pagesNumber), sizeof(pagesNumber));
        return 0;
    }
    return -1;
}

RC FileHandle::updateRecordCounter(){
    if(file_io_.is_open())
    {
        file_io_.seekp(sizeof(unsigned)*4);
        file_io_.write(reinterpret_cast<char *>(&recordCounter), sizeof(recordCounter));
        return 0;
    }
    return -1;
}

RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    readPageCount = this->readPageCounter;
    writePageCount = this->writePageCounter;
    appendPageCount = this->appendPageCounter;
    return 0;
}
