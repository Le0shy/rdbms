#include "rbfm.h"

// To make this file clean, libraries are better included in the header file
RecordBasedFileManager *RecordBasedFileManager::_rbf_manager = nullptr;
PagedFileManager *RecordBasedFileManager::pf_manager_ = nullptr;

RecordBasedFileManager *RecordBasedFileManager::instance() {
    if (!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

void RecordBasedFileManager::deleteInstance() {
    delete _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager() {
    pf_manager_ = PagedFileManager::instance();
}


RecordBasedFileManager::~RecordBasedFileManager() {
    pf_manager_->deleteInstance();
}

RC RecordBasedFileManager::createFile(const string &fileName) {
    return pf_manager_->createFile(fileName);
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
    return pf_manager_->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    return pf_manager_->openFile(fileName, fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return pf_manager_->closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle,
                                        const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {

    /* make sure fileHandle is bound to this file */

    /* process data and calculate the total space */
    Record record(data, recordDescriptor);
    uint16_t record_length = record.GetRecordLength();

    /* if it's larger than one page size */
    if (record_length + RecordPage::SIZE_TABLE_PAGE_TAIL + RecordPage::SIZE_SLOT > PAGE_SIZE) {
        return false;
    }
    /*  fetch a page with enough space */
    /*  fetch all the pages in buffer pool to find a page with enough space. */
    for (size_t i = 0; i < BUFFER_POOL_SIZE; i += 1) {
        auto page = static_cast<RecordPage *>(&(fileHandle.bp_manager_->GetPages()[i]));
        if (page->GetPageId() != INVALID_PAGE_ID) {
            if (page->InsertRecord(record, &rid)) {
                fileHandle.bp_manager_->page_directory_.
                        SetFreeSpace(page->GetPageId() - 2, page->GetFreeSpaceRemaining());
                fileHandle.recordCounter += 1;
                return 0;
            }
            // if false, keep fetch next frame in buffer pool
        }
    }

    /* otherwise scan whole page directory to find an available page. */
    /* If current page directory have no page with enough space, add a page */
    auto page = static_cast<RecordPage *>(fileHandle.FetchD(record_length));
    if (page != nullptr) {
        if (page->InsertRecord(record, &rid)) {
            fileHandle.bp_manager_->page_directory_.
                    SetFreeSpace(page->GetPageId() - 2, page->GetFreeSpaceRemaining());
            fileHandle.recordCounter += 1;
            return 0;
        }
    }

    page_id_t page_id{};
    auto new_page = static_cast<RecordPage * >(fileHandle.bp_manager_->NewPage(&page_id));
    if (new_page == nullptr) {
        throw ("new page failed");
    }
    if (!new_page->InsertRecord(record, &rid)) {
        return 1;
    }

    fileHandle.bp_manager_->page_directory_.
            SetFreeSpace(page_id - 2, new_page->GetFreeSpaceRemaining());
    fileHandle.pagesNumber += 1;
    fileHandle.bp_manager_->page_directory_.SetNumberAllocatedPages(fileHandle.pagesNumber);

    fileHandle.recordCounter += 1;

    return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle,
                                      const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    Record record;
    auto page = static_cast<RecordPage * >(fileHandle.bp_manager_->FetchPage(rid.pageNum));
    if (page == nullptr) return 1;

    if (page->IsTombStone(rid)) {
        Tombstone *tombstone = new Tombstone();
        page->ReadTombstone(rid, tombstone);

        RID actual_rid;
        actual_rid.pageNum = tombstone->GetActualPageID();
        actual_rid.slotNum = tombstone->GetActualSlotID();
        delete tombstone;

        auto actual_page = static_cast<RecordPage * >(fileHandle.bp_manager_->FetchPage(actual_rid.pageNum));
        if (!actual_page->ReadRecord(actual_rid, &record)) return 2;
    } else if (!page->ReadRecord(rid, &record)) return 3;

    record.DeserializeData(data, recordDescriptor);
    return 0;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    size_t offset = 0;
    const unsigned char *nullFieldsIndicator = reinterpret_cast<const unsigned char *>(data);

    offset += ceil((double) recordDescriptor.size() / CHAR_BIT);

    for (int attrIndex = 0; attrIndex < recordDescriptor.size(); ++attrIndex) {
        const Attribute &attr = recordDescriptor[attrIndex];
        bool isNull = nullFieldsIndicator[attrIndex / 8] & (1 << (7 - attrIndex % 8));
        if (isNull) {
            cout << attr.name << ": NULL ";
        } else {
            switch (attr.type) {
                case TypeInt: {
                    int intValue;
                    memcpy(&intValue, (char *) data + offset, sizeof(int));
                    cout << attr.name << ": " << intValue << " ";
                    offset += sizeof(int);
                    break;
                }
                case TypeReal: {
                    float floatValue;
                    memcpy(&floatValue, (char *) data + offset, sizeof(float));
                    cout << attr.name << ": " << floatValue << " ";
                    offset += sizeof(float);
                    break;
                }
                case TypeVarChar: {
                    int length;
                    memcpy(&length, (char *) data + offset, sizeof(int));
                    offset += sizeof(int);
                    string strValue((char *) data + offset, length);
                    cout << attr.name << ": " << strValue << " ";
                    offset += length;
                    break;
                }
            }
        }
    }
    cout << endl;
    return 0;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor,
                                        const RID &rid) {
    auto page = static_cast<RecordPage * >(fileHandle.Fetch(rid.pageNum));
    if (page == nullptr) return 1;

    /* if it's a tombstone, then get the actual rid from it, delete the real record */
    if (page->IsTombStone(rid)) {
        Tombstone *tombstone = new Tombstone();
        page->ReadTombstone(rid, tombstone);

        RID actual_rid;
        actual_rid.pageNum = tombstone->GetActualPageID();
        actual_rid.slotNum = tombstone->GetActualSlotID();
        delete tombstone;

        auto actual_page = static_cast<RecordPage *>(fileHandle.bp_manager_->FetchPage(actual_rid.pageNum));
        if (!actual_page->DeleteRecord(actual_rid)) return 2;
        fileHandle.bp_manager_->page_directory_.
        SetFreeSpace(actual_page->GetPageId() - 2, actual_page->GetFreeSpaceRemaining());
    }

    /* delete record at original rid */
    if (!page->DeleteRecord(rid)) return 3;
    fileHandle.bp_manager_->page_directory_.
            SetFreeSpace(page->GetPageId() - 2, page->GetFreeSpaceRemaining());
    fileHandle.recordCounter -= 1;
    return 0;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor,
                                        const void *data, const RID &rid) {
    RC rc;
    /* delete it first, no matter if it's a tombstone, cause deleteRecord will do everything ! */
    if ((rc = deleteRecord(fileHandle, recordDescriptor, rid)) != 0) return rc;

    Record new_record(data, recordDescriptor);
    auto original_page = static_cast<RecordPage *>(fileHandle.bp_manager_->FetchPage(rid.pageNum));
    auto free_space = original_page->GetFreeSpaceRemaining();

    if (new_record.GetRecordLength() <= free_space) {
        if (!original_page->UpdateRecord(new_record, rid)) return 1;
        fileHandle.recordCounter += 1;
    } else {
        RID new_rid{};
        if ((rc = insertRecord(fileHandle, recordDescriptor, data, new_rid)) != 0) return rc;
        Tombstone *tombstone = new Tombstone();
        tombstone->Initialize(new_rid);
        original_page->WriteTombstone(rid, tombstone);
//        fileHandle.bp_manager_->page_directory_.SetFreeSpace(
//                original_page ->GetPageId() - 2, original_page ->GetFreeSpaceRemaining());
        delete tombstone;
    }
    fileHandle.bp_manager_->page_directory_.
            SetFreeSpace(original_page->GetPageId() - 2,
                         original_page->GetFreeSpaceRemaining());
    return 0;
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor,
                                         const RID &rid, const string &attributeName, void *data) {
    size_t i = 0;
    size_t record_field_size = recordDescriptor.size();

    /* To know what attribute is being read */
    for (; i < record_field_size; i += 1) {
        if (recordDescriptor[i].name == attributeName) break;
    }
    if (i == record_field_size) return 1;

    /* read whole record to 'record' object */
    /* in case of triggering undefined behaviour, not use 'data' to read record from page */
    char *data_record = new char[PAGE_SIZE];
    memset(data_record, 0, PAGE_SIZE);
    RC rc = readRecord(fileHandle, recordDescriptor, rid, data_record);
    if (rc != 0) {
        delete[] data_record;
        return 2;
    }
    Record *record = new Record(data_record, recordDescriptor);
    delete[] data_record;

    /* read specific attribute from 'record' to 'data': get the length and offset */
    auto attribute_length = record->GetFieldOffset(i) - record->GetAttributeOffset(i);
    auto attribute_offset = record->GetDataOffset() + record->GetAttributeOffset(i);

    /* create null-indicators for attribute according to the demand from specification */
    char null_indicator = 0;
    if (attribute_length == 0) {
        null_indicator = 0x80;
    }
    memcpy(data, &null_indicator, sizeof(char));
    memcpy(reinterpret_cast<char *>(data) + sizeof(char), record->record_ + attribute_offset, attribute_length);

    delete record;
    return 0;
}

RC RecordBasedFileManager::scan(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor,
                                const string &conditionAttribute, const CompOp compOp, void *value,
                                const vector<string> &attributeNames, RBFM_ScanIterator &rbfm_ScanIterator) {
    rbfm_ScanIterator.Initialize(&fileHandle, recordDescriptor, conditionAttribute,
                                 compOp, value, attributeNames);

    return 0;
}

RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) {
    if (record_counter_ == record_number_) {
        return RBFM_EOF;
    }

    /* read record corresponding to 'rid_' from 'page_cure_' to 'record_'*/
    page_cur_->ReadRecord(rid_, &record_);
    record_counter_ += 1;

    /* if record does not satisfy the predication, keep update iterator */
    while (FilterRecord() != 0) {
        if (record_counter_ == record_number_) {
            return RBFM_EOF;
        }
        UpdateIterator();
        page_cur_->ReadRecord(rid_, &record_);
        record_counter_ += 1;
    }

    ProjRecord(data);
    rid.pageNum = rid_.pageNum;
    rid.slotNum = rid_.slotNum;

    UpdateIterator();

    return 0;
}

RC RBFM_ScanIterator::UpdateIterator() {
    /* if current page has no next record, get next page with valid record */
    if (!page_cur_->ReadNextRecordRid(cur_slot_id_, &next_slot_id_)) {
        GetNextPage();
        rid_.pageNum = page_counter_;
        rid_.slotNum = cur_slot_id_;
    }
    cur_slot_id_ = next_slot_id_;
    rid_.slotNum = next_slot_id_;
    return 0;
}

RC RBFM_ScanIterator::GetNextPage() {
    if (page_counter_ == page_number_) {
        return RBFM_EOF;
    }
    if (!page_cur_->ReadFirstRecordRid(&cur_slot_id_)) {
        page_counter_ += 1;
        page_cur_ = static_cast<RecordPage *>(file_handle_->Fetch(page_counter_));
        GetNextPage();
    }
    return 0;
}

RC RBFM_ScanIterator::FilterRecord() {
    /* if operator is NO_OP, every record is valid */
    if(com_op_ == NO_OP){
        return 0;
    }

    bool res;
    size_t attribute_length = 0;
    auto record_length = record_.GetRecordLength();
    char *attribute = new char[record_length];
    memset(attribute, 0, record_length);
    record_.ReadAttribute(condition_attr_, condition_attr_pos_,
                          attribute, &attribute_length);

    if (attribute_length == 0) {
        delete[] attribute;
        return 2;
    }

    switch (condition_attr_.type) {
        case TypeInt:
            res = Compare<int>(*reinterpret_cast<int *>(attribute), *reinterpret_cast<int *>(value_), com_op_);
            break;
        case TypeReal:
            res = Compare<float>(*reinterpret_cast<float *>(attribute), *reinterpret_cast<float *>(value_), com_op_);
            break;
        case TypeVarChar:
            char* actual_string = attribute + 4;
            string x(actual_string);
            string y(reinterpret_cast<char *>(value_));
            res = Compare<string>(x, y, com_op_);
            break;
    }

    delete[] attribute;
    if (res != true) {
        return 1;
    }
    return 0;
}

template<typename T>
bool RBFM_ScanIterator::Compare(T x, T y, CompOp op) const {
    std::function<bool(T, T)> comparator;
    switch (op) {
        case EQ_OP:
            comparator = [](T x, T y) { return x == y; };
            break;
        case LT_OP:
            comparator = [](T x, T y) { return x < y; };
            break;
        case LE_OP:
            comparator = [](T x, T y) { return x <= y; };
            break;
        case GT_OP:
            comparator = [](T x, T y) { return x > y; };
            break;
        case GE_OP:
            comparator = [](T x, T y) { return x >= y; };
            break;
        case NE_OP:
            comparator = [](T x, T y) { return x != y; };
            break;
        case NO_OP:
            comparator = [](T x, T y) { return true; };
            break;
        default:
            std::cerr << "Invalid operator \n" << std::endl;
            return false;
    }
    return comparator(x, y);
}

RC RBFM_ScanIterator::ProjRecord(void *data) {
    size_t proj_size = attr_names_.size();
    size_t null_length = ceil((double) proj_size / CHAR_BIT);
    size_t offset = null_length;
    size_t record_length = record_.GetRecordLength();
    size_t attribute_length = 0;

    char *dst = new char[record_length];
    memset(dst, 0, record_length);

    for (size_t i = 0; i < proj_size; i += 1) {
        for (size_t j = 0; i < rd_.size(); j += 1) {
            if (rd_[j].name == attr_names_[i]) {
                record_.ReadAttribute(rd_[j], j, dst + offset, &attribute_length);
                offset += attribute_length;
                if (attribute_length == 0) {
                    dst[i / CHAR_BIT] |= 1 << (7 - i % 8);
                }
                break;
            }
        }
    }
    memcpy(data, dst, offset);
    delete[] dst;
    return 0;
}

RC RBFM_ScanIterator::close() {
    return 0;
}


