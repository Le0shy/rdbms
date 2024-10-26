#include "dm.h"

DiskManager::DiskManager(const std::string &file_name) : file_name_(file_name)
{
    file_io_.open(file_name, std::ios::binary | std::ios::in | std::ios::out);
    // directory or file does not exist
    if (!file_io_.is_open())
    {
        file_io_.clear();
        // create a new file
        file_io_.open(file_name, std::ios::binary | std::ios::trunc | std::ios::out | std::ios::in);
        if (!file_io_.is_open())
        {
            throw("can't open db file");
        }
    }
}

DiskManager::~DiskManager()
{
    file_io_.close();
}

auto DiskManager::WritePage(page_id_t page_id, const void *data) -> RC
{
    size_t offset = page_id * PAGE_SIZE;
    file_io_.seekp(offset);
    file_io_.write(reinterpret_cast<const char *>(data), PAGE_SIZE);
    if (!file_io_.good())
    {
        std::cerr << "Error occurred when writing a page" << std::endl;
        return -1;
    }
    return 0;
}

auto DiskManager::ReadPage(page_id_t page_id, void *data) -> RC
{
    size_t offset = page_id * PAGE_SIZE;
    file_io_.seekg(offset);
    file_io_.read(reinterpret_cast<char *>(data), PAGE_SIZE);
    if (file_io_.bad())
    {
        std::cerr << "Error occurred when reading a page" << std::endl;
        return -1;
    }
    size_t read_size = file_io_.gcount();
    if (read_size < PAGE_SIZE)
    {
        std::cerr << "Read size is less than normal page size";
        file_io_.clear();
        return -1;
    }
    return 0;
}
