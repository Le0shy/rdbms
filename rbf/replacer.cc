#include "replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages):maximum_pages_(num_pages)
{
    
}

LRUReplacer::~LRUReplacer()
{
}

void LRUReplacer::RecordAccess(const frame_id_t *frame_id)
{
    auto it = std::find(LRU_list_.begin(), LRU_list_.end(), *frame_id);
    if(it != LRU_list_.end()){
        LRU_list_.erase(it);
    }
    LRU_list_.push_back(*frame_id);
}

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool
{
    if(LRU_list_.size() < maximum_pages_){
        throw("no need to evict page");
        return false;
    }
    *frame_id = LRU_list_.front();
    LRU_list_.pop_front();
    return true;
}
