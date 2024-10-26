# pragma once

# include <iostream>
# include <string>
# include <vector>
# include <cstdint>
# include <fstream>
# include "config.h"


/**
 * DiskManager takes care of the allocation and deallocation of pages within a database. It performs the reading and
 * writing of pages to and from disk, providing a logical file layer within the context of a database management system.
 */
class DiskManager {
    public:
    explicit DiskManager(const std::string& file_name);
    ~DiskManager();

    auto WritePage(page_id_t page_id, const void* data) -> RC;
    auto ReadPage(page_id_t page_id, void* data) -> RC;

    private: 
    std::fstream file_io_;
    std::string file_name_;

};