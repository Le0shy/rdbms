#include "bpm.h"

BufferPoolManager::BufferPoolManager(DiskManager *disk_manager) : disk_manager_(disk_manager)
{
    pages_ = new Page[BUFFER_POOL_SIZE];
    page_table_ = new std::unordered_map<page_id_t, frame_id_t>(BUFFER_POOL_SIZE);
    replacer_ = new LRUReplacer(BUFFER_POOL_SIZE);

    /* Initialize free_list_ and page_table_, buffer pool is empty*/
    for (size_t i = 0; i < BUFFER_POOL_SIZE; i += 1)
    {
        free_list_.push_back(i);
        //page_table_ -> insert({i,i});
    }

    disk_manager_ -> ReadPage (directory_id_, page_directory_.GetPage());

}

BufferPoolManager::~BufferPoolManager()
{
    delete[] pages_;
    delete page_table_;
    delete replacer_;
    delete disk_manager_;
}

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page *
{
    AllocatePage(*page_id);
    frame_id_t frame_id;

    if (free_list_.empty())
    {
        EvictHelper(frame_id);
    }
        if (free_list_.empty())
    {
        throw("free list is empty");
    }

    frame_id = free_list_.front();
    free_list_.pop_front();
    page_table_->insert({*page_id, frame_id});

    pages_[frame_id].Reset();
    pages_[frame_id].page_id_ = *page_id;
    replacer_->RecordAccess(&frame_id);

    return &pages_[frame_id];
}

auto BufferPoolManager::FetchHelper(page_id_t &page_id, frame_id_t &frame_id) -> Page *
{
    if (free_list_.empty())
    {
        throw("free list is empty");
    }
    frame_id = free_list_.front();
    free_list_.pop_front();
    page_table_->insert({page_id, frame_id});
    disk_manager_->ReadPage(page_id, pages_[frame_id].GetPage());
    pages_[frame_id].page_id_ = page_id;
    replacer_->RecordAccess(&frame_id);

    return &pages_[frame_id];
}

auto BufferPoolManager::EvictHelper(frame_id_t &frame_id) -> bool
{
    if (!free_list_.empty())
    {
        /* does not have to evict anything */
        throw("buffer pool is already available!");
    }

    /* find the victim frame_id and evicted_page_id*/
    replacer_->Victim(&frame_id);
    page_id_t evicted_page_id;

    for (const auto item : *page_table_)
    {
        if (item.second == frame_id)
        {
            evicted_page_id = item.first;
        }
    }

    /* add the frame to free_list_ and erase from page_table_ */
    free_list_.push_back(frame_id);

    /* if the page is dirty, write back to disk*/
    if (pages_[frame_id].IsDirty())
    {
        FlushPage(evicted_page_id);
    }

    /* Reset whole page to zero */
    page_table_->erase(evicted_page_id);
    pages_[frame_id].Reset();
    pages_[frame_id].page_id_ = INVALID_PAGE_ID;
    pages_[frame_id].is_dirty_ = false;

    return true;
}

auto BufferPoolManager::FetchPage(page_id_t page_id) -> Page *
{
    /* If the page needed is in the pool, record this access,
    and return pointer to the page. */
    frame_id_t frame_id;
    if (page_table_->find(page_id) != page_table_->end())
    {
        frame_id = page_table_->at(page_id);
        replacer_->RecordAccess(&frame_id);
        return &pages_[frame_id];
    }

    if (free_list_.empty())
    {
        EvictHelper(frame_id);
    }

    return FetchHelper(page_id, frame_id);
}

auto BufferPoolManager::FetchPageD(const uint16_t record_length) -> Page*{
    auto allocated_pages = page_directory_.GetNumberAllocatedPages();
    for(size_t i = 0; i < allocated_pages; i += 1){
        auto free_space = page_directory_.GetFreeSpace(i);
        if (free_space >= record_length){
            return FetchPage(i + 2);
        }
    }
    return nullptr;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool
{
    frame_id_t frame_id = (*page_table_)[page_id];
    disk_manager_->WritePage(page_id, pages_[frame_id].GetPage());
    return true;
}

auto BufferPoolManager::FlushAll() -> bool
{
    // size_t valid_pages = BUFFER_POOL_SIZE - free_list_.size();
    // for (auto item : *page_table_)
    // {
    //     if(valid_pages == 0){
    //         break;
    //     }
    //         FlushPage(item.first);
    //         valid_pages -= 1;
    // }
    // return true;
    for (size_t i = 0; i < BUFFER_POOL_SIZE; i += 1) {
        if (pages_[i].page_id_ != INVALID_PAGE_ID){
            disk_manager_-> WritePage(pages_[i].page_id_, pages_[i].GetPage());
        }
    }
    disk_manager_ -> WritePage(directory_id_, page_directory_.GetPage());
    return true;
}

auto BufferPoolManager::AllocatePage(page_id_t &allocate_page_id) -> page_id_t
{
    if (!page_directory_.Full())
    {
        auto allocated_pages = page_directory_.GetNumberAllocatedPages();
        page_directory_.SetNumberAllocatedPages(allocated_pages + 1);

        allocate_page_id = directory_id_ + allocated_pages + 1;

        return allocate_page_id;
    }
    else
    {
        page_id_t next_directory_id = page_directory_.GetNextDirectoryID();
        disk_manager_->WritePage(directory_id_, page_directory_.GetPage());

        page_directory_.Reset();
        if (next_directory_id == INVALID_PAGE_ID)
        {
            page_directory_.SetPrevDirectoryID(directory_id_);
            page_directory_.SetNumberAllocatedPages(0);
            page_directory_.SetNextDirectoryID(INVALID_PAGE_ID);
            directory_id_ += page_directory_.GetDirectoryLoad();
        }

        else
        {
            disk_manager_->ReadPage(next_directory_id, page_directory_.GetPage());
            directory_id_ = next_directory_id;
        }

        return AllocatePage(allocate_page_id);
    }
}
