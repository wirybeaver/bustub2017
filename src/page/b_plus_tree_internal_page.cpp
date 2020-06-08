/**
 * b_plus_tree_internal_page.cpp
 */
#include <iostream>

#include "common/exception.h"
#include "page/b_plus_tree_internal_page.h"

namespace cmudb {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id,
                                          page_id_t parent_id) {
    SetPageType(IndexPageType::INTERNAL_PAGE);
    SetSize(0);
    // -1 to reserve for an intermediate insertion
    SetMaxSize((PAGE_SIZE-sizeof(BPlusTreeInternalPage))/sizeof(MappingType)-1);
    SetParentPageId(parent_id);
    SetParentPageId(page_id);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
    assert(index >= 0 && index < GetSize());
    return array[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
    assert(index >= 0 && index < GetSize());
    array[index].first = key;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
    int i=0, n=GetSize();
    for (; i < n && ValueAt(i)!=value; i++){}
    return i==n? -1 : i;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const {
    assert(index >= 0 && index < GetSize());
    return array[index].second;
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType
B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key,
                                       const KeyComparator &comparator) const {
    assert(GetSize()>0);
    // find the index of largest Key  <= the target key
    int left=1, right = GetSize()-1;
    while(left<=right) {
        int mid = (right-left)/2+left;
        if(comparator(array[mid].first, key) <= 0) {
            left = mid+1;
        } else {
            right = mid-1;
        }
    }
    return array[right].second;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(
    const ValueType &old_value, const KeyType &new_key,
    const ValueType &new_value) {
    array[0].second = old_value;
    array[1].first = new_key;
    array[2].second = new_value;
    SetSize(2);
}
/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(
    const ValueType &old_value, const KeyType &new_key,
    const ValueType &new_value) {
    int idx = ValueIndex(old_value);
    int i=GetSize()-1;
    for(;i>idx;i--) {
        array[i+1].first = array[i].first;
        array[i+1].second = array[i].second;
    }
    array[idx+1].first = new_key;
    array[idx+1].second = new_value;
    IncreaseSize(1);
    return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(
    BPlusTreeInternalPage *recipient,
    BufferPoolManager *buffer_pool_manager) {
    int n = GetSize();
    assert(n==GetMaxSize()+1);
    int offset = n/2;
    recipient->CopyHalfFrom(array+offset, n-offset, buffer_pool_manager);
    SetSize(offset);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyHalfFrom(
    MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
    int idx = GetSize();
    assert(idx == 0);
    for(int i=0; i<size; i++, idx++) {
        auto *childRawPage = buffer_pool_manager->FetchPage(items[i].second);
        assert(childRawPage!=nullptr);
        auto *childTreePage = reinterpret_cast<BPlusTreePage *>(childRawPage->GetData());
        childTreePage->SetParentPageId(GetPageId());
        buffer_pool_manager->UnpinPage(items[i].second, true);
        array[idx].first = items[i].first;
        array[idx].second = items[i].second;
    }
    IncreaseSize(size);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
    int length = GetSize();
    assert(index >=0 && index<length);
    for(int i=index+1; i<length; i++) {
        array[i-1].first = array[i].first;
        array[i-1].second = array[i].second;
    }
    IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
    ValueType ans = ValueAt(0);
    IncreaseSize(-1);
    assert(GetSize()==0);
    return ans;
}
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page, then
 * update relevant key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(
    BPlusTreeInternalPage *recipient, int index_in_parent,
    BufferPoolManager *buffer_pool_manager) {
    int yourSize = recipient->GetSize(), mySize = GetSize();
    assert(yourSize+mySize<=recipient->GetMaxSize());

    // remove separating <key, value> pair
    auto *parentRawPage = buffer_pool_manager->FetchPage(GetParentPageId());
    assert(parentRawPage!= nullptr);
    auto *parentTreePage = reinterpret_cast<BPlusTreeInternalPage *>(parentRawPage->GetData());
    SetKeyAt(0, parentTreePage->KeyAt(index_in_parent));
    buffer_pool_manager->UnpinPage(GetParentPageId(), false);

    recipient->CopyAllFrom(array, mySize, buffer_pool_manager);
    SetSize(0);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyAllFrom(
    MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
    int length = GetSize();
    for(int i=0; i<size; i++) {
        array[length+i].first = items[i].first;
        array[length+i].second = items[i].second;
        auto *childRawPage = buffer_pool_manager->FetchPage(items[i].second);
        assert(childRawPage!=nullptr);
        auto *childTreePage = reinterpret_cast<BPlusTreePage *>(childRawPage->GetData());
        childTreePage->SetParentPageId(GetPageId());
        buffer_pool_manager->UnpinPage(childTreePage->GetPageId(), true);
    }
    IncreaseSize(size);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient"
 * page, then update relevant key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(
    BPlusTreeInternalPage *recipient,
    BufferPoolManager *buffer_pool_manager) {
    assert(1+recipient->GetSize()<=recipient->GetMaxSize());
    assert(GetSize()>1);

    auto *parentRawPage = buffer_pool_manager->FetchPage(GetParentPageId());
    auto *parentTreePage = reinterpret_cast<BPlusTreeInternalPage *>(parentRawPage->GetData());
    int index_in_parent = parentTreePage->ValueIndex(GetPageId());
    assert(index_in_parent>0);
    array[0].first = parentTreePage->KeyAt(index_in_parent);
    parentTreePage->SetKeyAt(index_in_parent, array[1].first);
    buffer_pool_manager->UnpinPage(parentTreePage->GetPageId(), true);

    recipient->CopyLastFrom(array[0], buffer_pool_manager);
    Remove(0);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(
    const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
    array[GetSize()].first = pair.first;
    array[GetSize()].second = pair.second;
    auto *childRawPage = buffer_pool_manager->FetchPage(pair.second);
    auto *childTreePage = reinterpret_cast<BPlusTreePage *>(childRawPage->GetData());
    childTreePage->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(childTreePage->GetPageId(), true);
    IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to head of "recipient"
 * page, then update relevant key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(
    BPlusTreeInternalPage *recipient, int parent_index,
    BufferPoolManager *buffer_pool_manager) {
    int last = GetSize()-1;
    assert(recipient->GetSize()+1 <= recipient->GetMaxSize());
    MappingType pair{KeyAt(last), ValueAt(last)};
    IncreaseSize(-1);
    recipient->CopyFirstFrom(pair, parent_index, buffer_pool_manager);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(
    const MappingType &pair, int parent_index,
    BufferPoolManager *buffer_pool_manager) {
    auto *parentRawPage = buffer_pool_manager->FetchPage(GetParentPageId());
    auto *parentTreePage = reinterpret_cast<BPlusTreeInternalPage *>(parentRawPage->GetData());
    array[0].first = parentTreePage->KeyAt(parent_index);
    parentTreePage->SetKeyAt(parent_index, pair.first);
    buffer_pool_manager->UnpinPage(GetParentPageId(), true);

    for(int i=GetSize(); i>0; i--){
        array[i].first = array[i-1].first;
        array[i].second = array[i-1].second;
    }

    array[0].first = pair.first;
    array[0].second = pair.second;
    IncreaseSize(1);
}

/*****************************************************************************
 * DEBUG
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::QueueUpChildren(
    std::queue<BPlusTreePage *> *queue,
    BufferPoolManager *buffer_pool_manager) {
  for (int i = 0; i < GetSize(); i++) {
    auto *page = buffer_pool_manager->FetchPage(array[i].second);
    if (page == nullptr)
      throw Exception(EXCEPTION_TYPE_INDEX,
                      "all page are pinned while printing");
    BPlusTreePage *node =
        reinterpret_cast<BPlusTreePage *>(page->GetData());
    queue->push(node);
  }
}

INDEX_TEMPLATE_ARGUMENTS
std::string B_PLUS_TREE_INTERNAL_PAGE_TYPE::ToString(bool verbose) const {
  if (GetSize() == 0) {
    return "";
  }
  std::ostringstream os;
  if (verbose) {
    os << "[pageId: " << GetPageId() << " parentId: " << GetParentPageId()
       << "]<" << GetSize() << "> ";
  }

  int entry = verbose ? 0 : 1;
  int end = GetSize();
  bool first = true;
  while (entry < end) {
    if (first) {
      first = false;
    } else {
      os << " ";
    }
    os << std::dec << array[entry].first.ToString();
    if (verbose) {
      os << "(" << array[entry].second << ")";
    }
    ++entry;
  }
  return os.str();
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t,
                                           GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t,
                                           GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t,
                                           GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t,
                                           GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t,
                                           GenericComparator<64>>;
} // namespace cmudb
