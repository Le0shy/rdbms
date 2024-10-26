#pragma once

#include <list>
#include <unordered_map>
#include "dm.h"
#include "page.h"
#include "replacer.h"

/**
 * BufferPoolManager reads disk pages to and from its internal buffer pool.
 */
class BufferPoolManager
{
friend class FileHandle;
friend class RecordBasedFileManager;
public:
    /**
     * @brief Creates a new BufferPoolManagerInstance.
     */
    explicit BufferPoolManager(DiskManager *disk_manager);

    /**
     * @brief Destroy an existing BufferPoolManagerInstance.
     */
    ~BufferPoolManager();

    /** @brief Return the pointer to all the pages in the buffer pool. */
    auto GetPages() -> Page * { return pages_; }

    auto GetPageDirectory() -> PageDirectory* {
        return &page_directory_;
    }

protected:
    /**
     * pick the replacement frame from either the free list or the replacer (always find from the free list
     * first), and then call the AllocatePage() method to get a new page id. If the replacement frame has a dirty page,
     * write it back to the disk first. reset the memory and metadata for the new page.
     *
     * @param[out] page_id id of created page
     * @return nullptr if no new pages could be created, otherwise pointer to new page
     */
    auto NewPage(page_id_t *page_id) -> Page *;

    /**
     * @brief Return the page in the first frame of free_list_,
     * insert {page_id, frame_id} to page_table, read corresponding
     * page via disk_manager_ and record frame_id's access
     * 
     * @param page_id, frame_id
     * @return Page* pointer to Page of #page_id
     */
    auto FetchHelper(page_id_t &page_id, frame_id_t &frame_id) -> Page *;
    /**
     * @brief Check if free_list_ is empty. If yes, find least recently used
     * frame and if it's dirty, write it back to disk, then evict it from buffer pool
     * 
     * @param frame_id 
     */
    auto EvictHelper(frame_id_t &frame_id) -> bool;

    /**
     * @brief Fetch the requested page from the buffer pool.
     *
     * First search for page_id in the buffer pool. If not found, pick a replacement frame from either the free list or
     * the replacer (always find from the free list first), read the page from disk by calling disk_manager_->ReadPage(),
     * and replace the old page in the frame. If the old page is dirty, write it back to disk and update the metadata of
     * the new page.
     *
     * @param page_id id of page to be fetched
     * @return nullptr if page_id cannot be fetched, otherwise pointer to the requested page
     */
    auto FetchPage(page_id_t page_id) -> Page *;

    auto FetchPageD(uint16_t record_length) -> Page*;

    /**
     * @brief Flush the target page to disk.
     * Use the DiskManager method to flush a page to disk, REGARDLESS of the dirty flag.
     * Unset the dirty flag of the page after flushing.
     *
     * @param page_id id of page to be flushed, cannot be INVALID_PAGE_ID
     * @return false if the page could not be found in the page table, true otherwise
     */
    auto FlushPage(page_id_t page_id) -> bool;

    /**
     *
     * @brief Flush all the pages in the buffer pool to disk.
     */
    auto FlushAll() -> bool;

    /**
     * @brief Allocate a page on disk. Caller should acquire the latch before calling this function.
     * @return the id of the allocated page
     */
    auto AllocatePage(page_id_t &allocate_page_id) -> page_id_t;

    /* Buffer Pool for this BPM. */
    Page *pages_;

    /* Page Directory for this BPM */
    PageDirectory page_directory_;

    Page page_hidden_;

    page_id_t directory_id_ = 1;

    /** Pointer to the disk manager. */
    DiskManager *disk_manager_ ;

    /** Page table for keeping track of buffer pool pages. */
    std::unordered_map<page_id_t, frame_id_t> *page_table_;

    /** Replacer to find pages for replacement. */
    LRUReplacer *replacer_ ;

    /** List of free frames that don't have any pages on them. */
    std::list<frame_id_t> free_list_;
};

