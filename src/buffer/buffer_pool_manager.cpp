#include "buffer/buffer_pool_manager.h"

namespace cmudb {

/*
 * BufferPoolManager Constructor
 * When log_manager is nullptr, logging is disabled (for test purpose)
 * WARNING: Do Not Edit This Function
 */
BufferPoolManager::BufferPoolManager(size_t pool_size,
                                                 DiskManager *disk_manager,
                                                 LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager),
      log_manager_(log_manager) {
  // a consecutive memory space for buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHash<page_id_t, Page *>(BUCKET_SIZE);
  replacer_ = new LRUReplacer<Page *>;
  free_list_ = new std::list<Page *>;

  // put all the pages into free list
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_->push_back(&pages_[i]);
  }
}

/*
 * BufferPoolManager Deconstructor
 * WARNING: Do Not Edit This Function
 */
BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
  delete free_list_;
}

/**
 * 1. search hash table.
 *  1.1 if exist, pin the page and return immediately
 *  1.2 if no exist, find a replacement entry from either free list or lru
 *      replacer. (NOTE: always find from free list first)
 * 2. If the entry chosen for replacement is dirty, write it back to disk.
 * 3. Delete the entry for the old page from the hash table and insert an
 * entry for the new page.
 * 4. Update page metadata, read page content from disk file and return page
 * pointer
 */
Page *BufferPoolManager::FetchPage(page_id_t page_id) {
    lock_guard<mutex> lck(latch_);
    Page *page = nullptr;
    if(page_table_->Find(page_id, page)) {
        page->pin_count_++;
        replacer_->Erase(page);
        return page;
    }
    page = GetFreeOrUnPinnedPage();
    if(page == nullptr) {
        return nullptr;
    }
    page_table_->Remove(page->GetPageId());
    page_table_->Insert(page_id, page);

    disk_manager_->ReadPage(page_id, page->data_);
    page->page_id_=page_id;
    page->is_dirty_ = false;
    page->pin_count_ = 1;
    return page;
}

/*
 * Implementation of unpin page
 * if pin_count>0, decrement it and if it becomes zero, put it back to
 * replacer if pin_count<=0 before this call, return false. is_dirty: set the
 * dirty flag of this page
 */
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
    lock_guard<mutex> lck(latch_);
    Page *page = nullptr;
    if(!page_table_->Find(page_id, page) || page== nullptr) {
        return false;
    }
    page->is_dirty_ = is_dirty;

    // don't understand why the comment would say pin_count can be less than 0
    if(page->pin_count_<=0) {
        return false;
    }
    if(--page->pin_count_==0) {
        replacer_->Insert(page);
    }
    return true;
}

/*
 * Used to flush a particular page of the buffer pool to disk. Should call the
 * write_page method of the disk manager
 * if page is not found in page table, return false
 * NOTE: make sure page_id != INVALID_PAGE_ID
 */
bool BufferPoolManager::FlushPage(page_id_t page_id) {
    lock_guard<mutex> lck(latch_);
    Page *page = nullptr;
    // don't understand why the comment would say page_id can be equals to INVALID_PAGE_ID
    if(!page_table_->Find(page_id, page) || page== nullptr || page->GetPageId()==INVALID_PAGE_ID) {
        return false;
    }
    if(page->is_dirty_) {
        disk_manager_->WritePage(page->GetPageId(), page->GetData());
        page->is_dirty_ = false;
    }
    return true;
}

/**
 * User should call this method for deleting a page. This routine will call
 * disk manager to deallocate the page. First, if page is found within page
 * table, buffer pool manager should be reponsible for removing this entry out
 * of page table, reseting page metadata and adding back to free list. Second,
 * call disk manager's DeallocatePage() method to delete from disk file. If
 * the page is found within page table, but pin_count != 0, return false
 */
bool BufferPoolManager::DeletePage(page_id_t page_id) {
    lock_guard<mutex> lck(latch_);
    Page *page = nullptr;
    page_table_->Find(page_id, page);
    if(page != nullptr) {
        if(page->GetPinCount() > 0) {
            return false;
        }
        page_table_->Remove(page_id);
        replacer_->Erase(page);
        page->ResetMemory();
        page->is_dirty_= false;
        page->page_id_=INVALID_PAGE_ID;
        free_list_->push_back(page);
    }
    disk_manager_->DeallocatePage(page_id);
    return true;
}

/**
 * User should call this method if needs to create a new page. This routine
 * will call disk manager to allocate a page.
 * Buffer pool manager should be responsible to choose a victim page either
 * from free list or lru replacer(NOTE: always choose from free list first),
 * update new page's metadata, zero out memory and add corresponding entry
 * into page table. return nullptr if all the pages in pool are pinned
 */
Page *BufferPoolManager::NewPage(page_id_t &page_id) {
    Page *page = GetFreeOrUnPinnedPage();
    if(page==nullptr) {
        return nullptr;
    }
    page_table_->Remove(page->GetPageId());
    page_table_->Insert(page_id, page);
    page_id = disk_manager_->AllocatePage();
    page->ResetMemory();
    page->page_id_ = page_id;
    page->is_dirty_ = false;
    page->pin_count_ = 1;
    return page;
}

/**
 * try to get one page from free_list_ first. If free_list_ is empty, find a victim page from LRUReplacer.
 * if the victim page is dirty, write the actual data back to disk.
 * @return
 */
Page *BufferPoolManager::GetFreeOrUnPinnedPage() {
    Page *ans;
    if(free_list_->empty()) {
        if(replacer_->Size() == 0) {
            return nullptr;
        } else {
            replacer_->Victim(ans);
            if(ans->is_dirty_) {
                disk_manager_->WritePage(ans->GetPageId(), ans->GetData());
            }

        }
    } else {
        ans = free_list_->back();
        free_list_->pop_back();
        assert(ans->GetPageId() == INVALID_PAGE_ID);
        assert(!ans->is_dirty_);
    }
    assert(ans->GetPinCount() == 0);

    return ans;
}

} // namespace cmudb
