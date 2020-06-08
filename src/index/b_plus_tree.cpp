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
    thread_local int BPLUSTREE_TYPE::rootLockedCnt = 0;

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
    bool BPLUSTREE_TYPE::IsEmpty() const {
        return root_page_id_ == INVALID_PAGE_ID;
    }
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
        auto *leafPage = FindLeafPage(key);
        if(leafPage == nullptr) {
            return false;
        }
        assert(!result.empty());
        bool ans = leafPage->Lookup(key, result[0], comparator_);
        FreePagesInTransaction(false, false, transaction);
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
        assert(transaction != nullptr);
        LockRootPageId(true);
        if (IsEmpty()) {
            StartNewTree(key, value);
            TryUnlockRootPageId(true);
            return true;
        }
        TryUnlockRootPageId(true);
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
        Page &page = *(buffer_pool_manager_->NewPage(pageId));
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
        auto *leafPage = FindLeafPage(key, false,BTreeOpType::INSERT, transaction);
        ValueType v;
        bool containsKey = leafPage->Lookup(key, v, comparator_);
        if (containsKey) {
            FreePagesInTransaction(true, false, transaction);
            return false;
        }
        leafPage->Insert(key, value, comparator_);
        if (leafPage->GetSize() > leafPage->GetMaxSize()) {
            auto *splittedRightPage = Split(leafPage, transaction);
            InsertIntoParent(leafPage, splittedRightPage->KeyAt(0), splittedRightPage, transaction);
        }
        FreePagesInTransaction(true, false, transaction);
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
    template<typename N>
    N *BPLUSTREE_TYPE::Split(N *node, Transaction *transaction) {
        page_id_t newPageId;
        auto *rawPage = buffer_pool_manager_->NewPage(newPageId);
        assert(rawPage != nullptr);
        rawPage->Latch(true);
        transaction->AddIntoPageSet(rawPage);
        N *btreeNode = reinterpret_cast<N *>(rawPage->GetData());
        btreeNode->Init(newPageId, node->GetParentPageId());
        node->MoveHalfTo(btreeNode, buffer_pool_manager_);
        return btreeNode;
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
            Page *rawPage = buffer_pool_manager_->NewPage(pageId);
            assert(rawPage != nullptr);
            auto newRootPage = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(rawPage->GetData());
            newRootPage->Init(pageId, INVALID_PAGE_ID);
            newRootPage->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
            new_node->SetParentPageId(pageId);
            old_node->SetParentPageId(pageId);
            root_page_id_ = pageId;
            UpdateRootPageId();
            buffer_pool_manager_->UnpinPage(pageId, true);
            return;
        }
        page_id_t pageId = old_node->GetParentPageId();
        // second pin
        auto *rawPage = buffer_pool_manager_->FetchPage(pageId);
        assert(rawPage != nullptr);
        new_node->SetParentPageId(pageId);
        auto *internalPage = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(rawPage->GetData());
        internalPage->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
        if (internalPage->GetSize() > internalPage->GetMaxSize()) {
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
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
    INDEX_TEMPLATE_ARGUMENTS
    void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
        LockRootPageId(true);
        if(IsEmpty()) {
            TryUnlockRootPageId(true);
            return;
        }
        TryUnlockRootPageId(true);
        B_PLUS_TREE_LEAF_PAGE_TYPE *bTreeLeafNode = FindLeafPage(key, false, BTreeOpType::DELETE, transaction);
        ValueType v;
        bool containsKey = bTreeLeafNode->Lookup(key, v, comparator_);
        if (!containsKey) {
            FreePagesInTransaction(true, false, transaction);
            return;
        }
        bTreeLeafNode->RemoveAndDeleteRecord(key, comparator_);
        if(bTreeLeafNode->GetSize() < bTreeLeafNode->GetMinSize()) {
            CoalesceOrRedistribute(bTreeLeafNode, transaction);
        }
        FreePagesInTransaction(true, false, transaction);
    }

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means coalesce happens
 */
    INDEX_TEMPLATE_ARGUMENTS
    template<typename N>
    bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
        if(node->IsRootPage()) {
            bool deleteRoot = AdjustRoot(node);
            assert(deleteRoot);
            transaction->AddIntoDeletedPageSet(node->GetPageId());
            return true;
        }
        // fetch twice. MUST unpin.
        Page *rawParentPage = buffer_pool_manager_ -> FetchPage(node->GetParentPageId());
        assert(rawParentPage!= nullptr);
        auto *bTreeParentNode = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(rawParentPage -> GetData());
        int in_parent_idx = bTreeParentNode -> ValueIndex(node->GetPageId());
        Page *rawLeftSiblingPage = nullptr, *rawRightSiblingPage = nullptr;
        N *bTreeLeftSiblingNode = nullptr, *bTreeRightSiblingNode = nullptr;
        if(in_parent_idx > 0) {
            rawLeftSiblingPage = buffer_pool_manager_ -> FetchPage(bTreeParentNode -> ValueAt(in_parent_idx-1));
            rawLeftSiblingPage->Latch(true);
            bTreeLeftSiblingNode = reinterpret_cast<N *>(rawLeftSiblingPage -> GetData());
            if(bTreeLeftSiblingNode->GetSize() > bTreeLeftSiblingNode->GetMinSize()) {
                transaction->AddIntoPageSet(rawLeftSiblingPage);
                buffer_pool_manager_-> UnpinPage(bTreeParentNode->GetPageId(), false);
                Redistribute(bTreeLeftSiblingNode, node, in_parent_idx);
                assert(rawParentPage->GetPinCount()>0);
                return false;
            }
        }
        if(in_parent_idx < bTreeParentNode->GetSize()-1) {
            rawRightSiblingPage = buffer_pool_manager_ -> FetchPage(bTreeParentNode -> ValueAt(in_parent_idx+1));
            rawRightSiblingPage->Latch(true);
            bTreeRightSiblingNode = reinterpret_cast<N *>(rawRightSiblingPage -> GetData());
            if(bTreeRightSiblingNode->GetSize() > bTreeRightSiblingNode->GetMinSize()) {
                if(bTreeLeftSiblingNode!= nullptr) {
                    rawLeftSiblingPage->UnLatch(true);
                    buffer_pool_manager_->UnpinPage(bTreeLeftSiblingNode->GetPageId(), false);
                }
                transaction->AddIntoPageSet(rawRightSiblingPage);
                buffer_pool_manager_-> UnpinPage(bTreeParentNode->GetPageId(), false);
                Redistribute(bTreeRightSiblingNode, node, 0);
                assert(rawParentPage->GetPinCount()>0);
                return false;
            }
        }

        // Coalesce
        if(in_parent_idx > 0) {
            assert(rawLeftSiblingPage != nullptr && bTreeLeftSiblingNode!= nullptr && rawLeftSiblingPage->GetPageId()==bTreeLeftSiblingNode->GetPageId());
            if(bTreeRightSiblingNode != nullptr) {
                assert(rawRightSiblingPage!= nullptr && rawRightSiblingPage->GetPageId() == bTreeRightSiblingNode->GetPageId());
                rawRightSiblingPage->UnLatch(true);
                buffer_pool_manager_->UnpinPage(bTreeRightSiblingNode->GetPageId(), false);
            }
            transaction->AddIntoPageSet(rawLeftSiblingPage);
            buffer_pool_manager_-> UnpinPage(bTreeParentNode->GetPageId(), false);
            assert(rawParentPage->GetPinCount()>0);
            Coalesce(bTreeLeftSiblingNode, node, bTreeParentNode, in_parent_idx, transaction);
            return true;
        } else {
            assert(rawRightSiblingPage != nullptr && bTreeRightSiblingNode != nullptr && rawRightSiblingPage->GetPageId()==bTreeRightSiblingNode->GetPageId());
            assert(rawLeftSiblingPage == nullptr && bTreeLeftSiblingNode==nullptr);
            transaction->AddIntoPageSet(rawLeftSiblingPage);
            buffer_pool_manager_-> UnpinPage(bTreeParentNode->GetPageId(), false);
            assert(rawParentPage->GetPinCount()>0);
            Coalesce(node, bTreeRightSiblingNode, bTreeParentNode, in_parent_idx+1, transaction);
            return true;
        }
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
    template<typename N>
    bool BPLUSTREE_TYPE::Coalesce(
            N *&neighbor_node, N *&node,
            BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *&parent,
            int index, Transaction *transaction) {
        assert(node->GetSize() + node->GetSize() <= node->GetMaxSize());
        // assumption, neighbor_node is always before node
        node->MoveAllTo(neighbor_node, index, buffer_pool_manager_);
        transaction->AddIntoDeletedPageSet(node->GetPageId());
        parent->Remove(index);
        if(parent->GetSize() < parent->GetMinSize()) {
            return CoalesceOrRedistribute(parent, transaction);
        }
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
    template<typename N>
    void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
        if(index == 0) {
            //right sibling
            neighbor_node->MoveFirstToEndOf(node, buffer_pool_manager_);
        } else {
            neighbor_node->MoveLastToFrontOf(node, index, buffer_pool_manager_);
        }
    }
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
        if(old_root_node->IsLeafPage()) {
            //case 2
            root_page_id_ = INVALID_PAGE_ID;
            UpdateRootPageId();
            return true;
        }
        // case 1
        assert(old_root_node->GetSize()==1);
        auto *bTreeInternalNode = static_cast<B_PLUS_TREE_INTERNAL_PAGE *>(old_root_node);
        page_id_t newRootId = bTreeInternalNode->RemoveAndReturnOnlyChild();
        auto *rawPage = buffer_pool_manager_->FetchPage(newRootId);
        assert(rawPage != nullptr);
        auto *newBTreeRootNode = reinterpret_cast<BPlusTreePage *>(rawPage->GetData());
        newBTreeRootNode -> SetParentPageId(INVALID_PAGE_ID);
        buffer_pool_manager_->UnpinPage(newRootId, true);
        return true;
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
    INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() {
        KeyType dummy;
        auto *firstLeafPage = FindLeafPage(dummy, true);
        TryUnlockRootPageId(false);
        return INDEXITERATOR_TYPE(firstLeafPage, buffer_pool_manager_, 0);
    }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
    INDEX_TEMPLATE_ARGUMENTS
    INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
        auto *firstLeafPage = FindLeafPage(key);
        TryUnlockRootPageId(false);
        if(firstLeafPage == nullptr) {
            return INDEXITERATOR_TYPE(nullptr, buffer_pool_manager_, 0);
        }
        int idx = firstLeafPage->KeyIndex(key, comparator_);
        return INDEXITERATOR_TYPE(firstLeafPage, buffer_pool_manager_, idx);
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
                                                             bool leftMost,
                                                             BTreeOpType op,
                                                             Transaction *transaction) {
        bool exclusive = op != BTreeOpType::READ;
        LockRootPageId(exclusive);
        if (IsEmpty()) {
            TryUnlockRootPageId(exclusive);
            return nullptr;
        }
        auto *bTreeNode = CrabbingFetchPage(root_page_id_, -1, op, transaction);
        while (!bTreeNode->IsLeafPage()) {
            auto *internalNode = static_cast<B_PLUS_TREE_INTERNAL_PAGE *>(bTreeNode);
            page_id_t child = leftMost? internalNode->ValueAt(0) : internalNode->Lookup(key, comparator_);
            bTreeNode = CrabbingFetchPage(child, bTreeNode->GetPageId(), op, transaction);
        }
        return static_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(bTreeNode);
    }

    INDEX_TEMPLATE_ARGUMENTS
    BPlusTreePage *BPLUSTREE_TYPE::CrabbingFetchPage(page_id_t child, page_id_t parent, BTreeOpType op, Transaction *transaction){
        bool exclusive = op != BTreeOpType::READ;
        auto *childRawPage = buffer_pool_manager_->FetchPage(child);
        assert(childRawPage != nullptr);
        childRawPage->Latch(exclusive);
        auto *childBTreeNode = reinterpret_cast<BPlusTreePage *>(childRawPage->GetData());
        if(childBTreeNode ->IsSafe(op)) {
            if(transaction == nullptr) {
                // called by iterator
                if(parent!=INVALID_PAGE_ID) {
                    Page *parentRawPage = buffer_pool_manager_->FetchPage(parent);
                    buffer_pool_manager_->UnpinPage(parentRawPage->GetPageId(), false);
                    assert(parentRawPage->GetPinCount()>0);
                    parentRawPage->UnLatch(exclusive);
                    buffer_pool_manager_->UnpinPage(parentRawPage->GetPageId(), false);
                }
            } else {
                FreePagesInTransaction(exclusive, true, transaction);
            }
        }
        if(transaction != nullptr) {
            transaction->AddIntoPageSet(childRawPage);
        }
        return childBTreeNode;
    }

    INDEX_TEMPLATE_ARGUMENTS
    void BPLUSTREE_TYPE::FreePagesInTransaction(bool exclusive, bool findLeafPageOngoing, Transaction *transaction) {
        assert(transaction!= nullptr);
        TryUnlockRootPageId(exclusive);
        bool likelyDirty = exclusive && !findLeafPageOngoing;
        for( Page *page : *transaction->GetPageSet()) {
            page->UnLatch(exclusive);
            buffer_pool_manager_->UnpinPage(page->GetPageId(), likelyDirty);
            if(transaction->GetDeletedPageSet()->count(page->GetPageId()) > 0) {
                buffer_pool_manager_->DeletePage(page->GetPageId());
                transaction->GetDeletedPageSet()->erase(page->GetPageId());
            }
        }
        transaction->GetPageSet()->clear();
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

    INDEX_TEMPLATE_ARGUMENTS
    void BPLUSTREE_TYPE::LockRootPageId(bool exclusive) {
        if (exclusive) {
            mutex_.WLock();
        } else {
            mutex_.RLock();
        }
        rootLockedCnt++;
    }

    INDEX_TEMPLATE_ARGUMENTS
    void BPLUSTREE_TYPE::TryUnlockRootPageId(bool exclusive) {
        if (rootLockedCnt > 0) {
            if (exclusive) {
                mutex_.WUnlock();
            } else {
                mutex_.RUnlock();
            }
            rootLockedCnt--;
        }
    }

    template
    class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

    template
    class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

    template
    class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

    template
    class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

    template
    class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace cmudb
