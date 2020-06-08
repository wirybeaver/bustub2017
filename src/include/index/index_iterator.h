/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "page/b_plus_tree_leaf_page.h"

namespace cmudb {

#define INDEXITERATOR_TYPE                                                     \
  IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
public:
  // you may define your own constructor based on your member variables
  IndexIterator();
  IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE *firstLeafPage, BufferPoolManager *bufferPoolManager, int idx) : cur_leaf_page_(firstLeafPage), buffer_pool_manager_(bufferPoolManager), index_(idx){
      // redundant pin here. The first pin happens at FindLeafPage() as well as the Latch.
      if(cur_leaf_page_ != nullptr) {
          raw_page_ = buffer_pool_manager_->FetchPage(cur_leaf_page_->GetPageId());
          bufferPoolManager->UnpinPage(cur_leaf_page_->GetPageId(), false);
          assert(raw_page_->GetPinCount()>0);
      }
  }
  ~IndexIterator();

  bool isEnd() {
      return cur_leaf_page_ == nullptr;
  }

  const MappingType &operator*() {
      return cur_leaf_page_->GetItem(index_);
  }

  IndexIterator &operator++() {
      index_++;
      if(index_>=cur_leaf_page_->GetSize()){
          page_id_t nextPageId = cur_leaf_page_->GetNextPageId();
          raw_page_->UnLatch(false);
          buffer_pool_manager_->UnpinPage(cur_leaf_page_->GetPageId(), false);
          if(nextPageId == INVALID_PAGE_ID) {
              cur_leaf_page_ = nullptr;
          } else {
              raw_page_ = buffer_pool_manager_->FetchPage(nextPageId);
              raw_page_->Latch(false);
              cur_leaf_page_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(raw_page_->GetData());
              index_ = 0;
          }
      }
      return *this;
  }

private:
  // add your own private member variables here
  Page *raw_page_;
  B_PLUS_TREE_LEAF_PAGE_TYPE *cur_leaf_page_;
  BufferPoolManager *buffer_pool_manager_;
  int index_;
};

} // namespace cmudb
