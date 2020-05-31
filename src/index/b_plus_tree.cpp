/**
 * b_plus_tree.cpp
 */
#include <iostream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "index/b_plus_tree.h"
#include "page/header_page.h"

namespace cmudb {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(const std::string &name,
                                BufferPoolManager *buffer_pool_manager,
                                const KeyComparator &comparator,
                                page_id_t root_page_id)
    : index_name_(name), root_page_id_(root_page_id),
      buffer_pool_manager_(buffer_pool_manager), comparator_(comparator) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { return root_page_id_==INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key,
                              std::vector<ValueType> &result,
                              Transaction *transaction) {
    if(IsEmpty()) {
        return false;
    }
    auto *leafPage = FindLeafPage(key);
    bool ans = leafPage->Lookup(key, result[0], comparator_);
    buffer_pool_manager_->UnpinPage(leafPage->GetPageId(), false);
    return ans;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value,
                            Transaction *transaction) {
    if(IsEmpty()) {
      StartNewTree(key, value);
      return true;
    }
    return InsertIntoLeaf(key, value, transaction);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
    // page initialization
    page_id_t pageId;
    Page & page = *(buffer_pool_manager_->NewPage(pageId));
    assert((&page) != nullptr);

    // b+ tree initialization
    B_PLUS_TREE_LEAF_PAGE_TYPE &root = *(reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page.GetData()));
    root.Init(pageId, INVALID_PAGE_ID);
    root.Insert(key, value, comparator_);
    // update root page id
    root_page_id_ = pageId;
    UpdateRootPageId(true);
    buffer_pool_manager_->UnpinPage(pageId, true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value,
                                    Transaction *transaction) {
    // assumption: the tree is not empty.
  auto *leafPage = FindLeafPage(key);
  ValueType v;
  bool containsKey = leafPage->Lookup(key, v, comparator_);
  if (containsKey) {
      buffer_pool_manager_->UnpinPage(leafPage->GetPageId(), false);
      return false;
  }
  leafPage->Insert(key, value, comparator_);
  if(leafPage->GetSize() > leafPage->GetMaxSize()) {
      auto *splittedRightPage = Split(leafPage);
      InsertIntoParent(leafPage, splittedRightPage->KeyAt(0), splittedRightPage, transaction);
  }
  buffer_pool_manager_->UnpinPage(leafPage->GetPageId(), true);
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N> N *BPLUSTREE_TYPE::Split(N *node) {
    page_id_t newPageId;
    auto *phypage = buffer_pool_manager_->NewPage(newPageId);
    assert(phypage!=nullptr);
    N *newNode = reinterpret_cast<N *>(phypage->GetData());
    newNode->Init(newPageId, node->GetParentPageId());
    node->MoveHalfTo(newNode, buffer_pool_manager_);
    return newNode;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node,
                                      const KeyType &key,
                                      BPlusTreePage *new_node,
                                      Transaction *transaction) {
    if (old_node->IsRootPage()) {
        page_id_t pageId;
        Page* phyPage = buffer_pool_manager_->NewPage(pageId);
        assert(phyPage != nullptr);
        auto newRootPage = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(phyPage->GetData());
        newRootPage->Init(pageId, INVALID_PAGE_ID);
        newRootPage->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
        new_node->SetParentPageId(pageId);
        old_node->SetParentPageId(pageId);
        root_page_id_ = pageId;
        UpdateRootPageId();
        buffer_pool_manager_->UnpinPage(root_page_id_, true);
        buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
        return;
    }
    page_id_t pageId = old_node->GetParentPageId();
    auto *physicalPage = buffer_pool_manager_->FetchPage(pageId);
    assert(physicalPage != nullptr);
    new_node->SetParentPageId(pageId);
    buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
    auto *internalPage = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(physicalPage->GetData());
    internalPage->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
    if(internalPage->GetSize() > internalPage->GetMaxSize()) {
        auto *splittedRightPage = Split(internalPage);
        InsertIntoParent(internalPage, splittedRightPage->KeyAt(0), splittedRightPage, transaction);
    }
    buffer_pool_manager_->UnpinPage(pageId, true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  return false;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(
    N *&neighbor_node, N *&node,
    BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *&parent,
    int index, Transaction *transaction) {
  return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  return INDEXITERATOR_TYPE();
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
B_PLUS_TREE_LEAF_PAGE_TYPE *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key,
                                                         bool leftMost) {
    if(IsEmpty()) {
        return nullptr;
    }
    page_id_t cur = root_page_id_;
    auto *phyPage = buffer_pool_manager_->FetchPage(cur);
    assert(phyPage!= nullptr);
    auto *node = reinterpret_cast<BPlusTreePage *>(phyPage->GetData());
    while(!node->IsLeafPage()) {
        page_id_t next;
        auto *internalNode = static_cast<B_PLUS_TREE_INTERNAL_PAGE *>(node);
        if(leftMost) {
            next = internalNode->ValueAt(0);
        } else {
            next = internalNode->Lookup(key, comparator_);
        }
        phyPage = buffer_pool_manager_->FetchPage(next);
        buffer_pool_manager_->UnpinPage(cur, false);
        assert(phyPage!= nullptr);
        node = reinterpret_cast<BPlusTreePage *>(phyPage->GetData());
        cur = next;
    }
    return static_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(node);
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(
      buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record)
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  else
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for debug only
 * print out whole b+tree sturcture, rank by rank
 */
INDEX_TEMPLATE_ARGUMENTS
std::string BPLUSTREE_TYPE::ToString(bool verbose) { return "Empty tree"; }

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name,
                                    Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name,
                                    Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace cmudb
