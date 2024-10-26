#pragma once

#include <cstring>
#include <iostream>
#include <cstring>
#include "config.h"
#include "record.h"

class Page {
    friend class BufferPoolManager;

public:
    /** Constructor. Zeros out the page data. */
    Page() { Reset(); }

    /** Default destructor. */
    ~Page() = default;

    /** @return the actual data contained within this page */
    inline auto GetPage() -> char * { return page_block_; }

    /** @return the page id of this page */
    inline auto GetPageId() -> page_id_t { return page_id_; }

    /** @return true if the page in memory has been modified from the page on disk, false otherwise */
    inline auto IsDirty() -> bool { return is_dirty_; };

    void SetDirty() {
        is_dirty_ = true;
    };

protected:
    static_assert(sizeof(page_id_t) == 4);

    static constexpr size_t OFFSET_PAGE_START = 0;

private:
    /** Zeroes out the data that is held within the page. */
    inline void Reset() { memset(page_block_, OFFSET_PAGE_START, PAGE_SIZE); }

    /** The actual data that is stored within a page. */
    char page_block_[PAGE_SIZE]{};
    /** The ID of this page. */
    page_id_t page_id_ = INVALID_PAGE_ID;
    /** True if the page is dirty, i.e. it is different from its corresponding page on disk. */
    bool is_dirty_ = false;
};

class RecordPage : public Page {
    friend class RecordBasedFileManager;

public:
    RecordPage() : Page() {

    }

    /**
    * Insert a record into the table.
    * @param record record to insert
    * @param[out] rid rid of the inserted record
    * @return true if the insert is successful (i.e. there is enough space)
 */
    auto InsertRecord(Record &record, RID *rid) -> bool;

    /**
     * Mark a record as deleted. This does not actually delete the record.
     * @param rid rid of the record to mark as deleted
     * @return true if marking the record as deleted is successful (i.e the record exists)
     */
    auto DeleteRecord(const RID &rid) -> bool;

    /**
     * Update a record.
     * @param new_record new value of the record
     * @param[out] old_record old value of the record
     * @param rid rid of the record
     * @return true if updating the record succeeded
     */
    auto UpdateRecord(const Record &new_record, const RID &rid) -> bool;

    /** To be called on commit or abort. Actually perform the delete or rollback an insert. */
    void DeleteHelper(const RID &rid);

    auto IsTombStone(const RID &rid) -> bool{
        return GetRecordSize(rid.slotNum) == TOMBSTONE_INDICATOR;
    }

    void ReadTombstone(const RID&rid, Tombstone* tombstone);

    void WriteTombstone(const RID&rid, Tombstone* tombstone);

    /**
     * Read a record from a table.
     * @param rid rid of the record to read
     * @param[out] record the record that was read
     * @return true if the read is successful (i.e. the record exists)
     */
    auto ReadRecord(const RID &rid, Record *record) -> bool;

    /**
     * @param[out] first_rid the RID of the first record in this page
     * @return true if the first record exists, false otherwise
     */
    auto ReadFirstRecordRid(slot_id_t *first_rid) -> bool;

    /**
     * @param cur_rid the RID of the current record
     * @param[out] next_rid the RID of the record following the current record
     * @return true if the next record exists, false otherwise
     */
    auto ReadNextRecordRid(const slot_id_t &cur_rid, slot_id_t *next_rid) -> bool;

private:
    static_assert(sizeof(page_id_t) == 4);

    static constexpr uint16_t TOMBSTONE_INDICATOR = -1;
    static constexpr size_t SIZE_TABLE_PAGE_TAIL = 4;
    static constexpr size_t SIZE_SLOT = 4;
    static constexpr size_t OFFSET_FREE_SPACE = PAGE_SIZE - 2;
    static constexpr size_t OFFSET_SLOT_COUNT = PAGE_SIZE - 4;
    static constexpr size_t OFFSET_RECORD_OFFSET = PAGE_SIZE - 8;
    static constexpr size_t OFFSET_RECORD_SIZE = PAGE_SIZE - 6;

    /** @return pointer to the end of the current free space, see header comment */
    auto GetFreeSpacePointer() -> uint16_t { return *reinterpret_cast<uint16_t *>(GetPage() + OFFSET_FREE_SPACE); }

    /** Sets the pointer, this should be the end of the current free space. */
    void SetFreeSpacePointer(uint16_t free_space_pointer) {
        memcpy(GetPage() + OFFSET_FREE_SPACE, &free_space_pointer, sizeof(uint16_t));
    }

    /**
     * @note returns; slot count, some slots may be empty
     * @return at least the number of records in this page
     */
    auto GetSlotCount() -> uint16_t { return *reinterpret_cast<uint16_t *>(GetPage() + OFFSET_SLOT_COUNT); }

    /** Set the number of slots in this page. */
    void SetSlotCount(uint16_t record_count) { memcpy(GetPage() + OFFSET_SLOT_COUNT, &record_count, sizeof(uint16_t)); }

    /** Get offset of slot of #slot_num */
    auto GetSlot(uint16_t slot_num) -> uint16_t {
        if (slot_num < GetSlotCount()) {
            slot_num += 1;
            return uint16_t(OFFSET_SLOT_COUNT) - slot_num * uint16_t(SIZE_SLOT);
        }
        return -1;
    }

    /** Get free space remaining in this page*/
    auto GetFreeSpaceRemaining() -> uint16_t {
        return GetSlot(GetSlotCount() - 1) - GetFreeSpacePointer();
    }

    /** @return record offset at slot slot_num */
    auto GetRecordOffsetAtSlot(uint16_t slot_num) -> uint16_t {
        return *reinterpret_cast<uint16_t *>(GetPage() + GetSlot(slot_num));
    }

    /** Set record offset at slot slot_num. */
    void SetRecordOffsetAtSlot(uint16_t slot_num, uint16_t offset) {
        memcpy(GetPage() + GetSlot(slot_num), &offset, sizeof(uint16_t));
    }

    /** @return record size at slot slot_num */
    auto GetRecordSize(uint16_t slot_num) -> uint16_t {
        return *reinterpret_cast<uint16_t *>(GetPage() + GetSlot(slot_num) + sizeof(uint16_t));
    }

    /** Set record size at slot slot_num. */
    void SetRecordSize(uint16_t slot_num, uint16_t size) {
        memcpy(GetPage() + GetSlot(slot_num) + sizeof(uint16_t), &size, sizeof(uint16_t));
    }
};


class PageDirectory : public Page {
public:
    PageDirectory() : Page() {
        SetPrevDirectoryID(INVALID_PAGE_ID);
        SetNextDirectoryID(INVALID_PAGE_ID);
        SetNumberAllocatedPages(0);
    }

protected:
    //size_t num_allocated_pages{};

    static constexpr size_t SIZE_DIRECTORY_HEADER = 12;
    static constexpr size_t SIZE_OFFSET_PREV_DIR = 4;
    static constexpr size_t SIZE_OFFSET_NEXT_DIR = 4;
    static constexpr size_t SIZE_FREE_SPACE = 2;
    static constexpr size_t OFFSET_PREV_DIR = 0;
    static constexpr size_t OFFSET_NEXT_DIR = 4;
    static constexpr size_t OFFSET_ALLOCATED_PAGES = 8;

    static constexpr uint32_t MAX_ALLOCATED_PER_DIRECTORY = 2042;

public:
    // maybe use functional programming here would be more efficient and readable
    auto GetPrevDirectoryID() -> uint32_t {
        return *reinterpret_cast<uint32_t *>(GetPage() + OFFSET_PREV_DIR);
    }

    void SetPrevDirectoryID(page_id_t prev_page_id) {
        memcpy(GetPage() + OFFSET_PREV_DIR, &prev_page_id, SIZE_OFFSET_PREV_DIR);
    }

    auto GetNextDirectoryID() -> uint32_t {
        return *reinterpret_cast<uint32_t *>(GetPage() + OFFSET_NEXT_DIR);
    }

    void SetNextDirectoryID(page_id_t next_page_id) {
        memcpy(GetPage() + OFFSET_NEXT_DIR, &next_page_id, SIZE_OFFSET_NEXT_DIR);
    }

    auto GetFreeSpace(page_id_t directory_page_id) -> uint16_t {
        return *reinterpret_cast<uint16_t *>(GetPage() + SIZE_DIRECTORY_HEADER + directory_page_id * SIZE_FREE_SPACE);
    }

    void SetFreeSpace(page_id_t directory_page_id, const uint16_t free_space) {
        std::cout << "free space of " << directory_page_id <<  ":" << free_space << std::endl;
        memcpy(GetPage() + SIZE_DIRECTORY_HEADER + directory_page_id * SIZE_FREE_SPACE, &free_space, sizeof(uint16_t));
    }

    auto GetNumberAllocatedPages() -> uint32_t {
        return *reinterpret_cast<uint32_t *>(GetPage() + OFFSET_ALLOCATED_PAGES);
    }

    void SetNumberAllocatedPages(const uint32_t num_allocated_pages) {
        memcpy(GetPage() + OFFSET_ALLOCATED_PAGES, &num_allocated_pages, sizeof(uint32_t));
    }

    auto GetDirectoryLoad() -> uint32_t {
        return MAX_ALLOCATED_PER_DIRECTORY;
    }

    auto Full() -> bool {
        if (GetNumberAllocatedPages() == MAX_ALLOCATED_PER_DIRECTORY) return true;
        else return false;
    }

};