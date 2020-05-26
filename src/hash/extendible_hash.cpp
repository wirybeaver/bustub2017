#include <list>

#include "hash/extendible_hash.h"
#include "page/page.h"

class share_lock;
namespace cmudb {

/*
 * constructor
 * array_size: fixed array size for each bucket
 */
template <typename K, typename V>
ExtendibleHash<K, V>::ExtendibleHash(size_t size) {}

/*
 * helper function to calculate the hashing address of input key
 */
template <typename K, typename V>
size_t ExtendibleHash<K, V>::HashKey(const K &key) {
    return hash<K>{}(key);
}

/*
 * helper function to return global depth of hash table
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetGlobalDepth() const {
    lock_guard<mutex> lock(latch);
    return globalDepth;
}

/*
 * helper function to return local depth of one specific bucket
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetLocalDepth(int bucket_id) const {
    unique_lock<mutex> globalLock(latch);
    if (buckets[bucket_id]) {
        unique_lock<mutex> bucketLock(buckets[bucket_id]->latch);
        globalLock.unlock();
        {
            return buckets[bucket_id]->localDepth;
        }
    }
    return -1;
}

/*
 * helper function to return current number of bucket in hash table
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetNumBuckets() const {
    lock_guard<mutex> lock(latch);
    return bucketNum;
}

/*
 * lookup function to find value associate with input key
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Find(const K &key, V &value) {
    unique_lock<mutex> globalLock(latch);
    int pos = getIdx(key);
    unique_lock<mutex> localLock(buckets[pos]->latch);
    globalLock.unlock();
    auto iter = buckets[pos]->mp.find(key);
    if(iter!=buckets[pos]->mp.end()) {
        value = iter->second;
        return true;
    } else {
        return false;
    }
}

/*
 * delete <key,value> entry in hash table
 * Shrink & Combination is not required for this project
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Remove(const K &key) {
  return false;
}

template <typename K, typename V>
int ExtendibleHash<K, V>::getIdx(const K &key) {
    return HashKey(key) & ((1u<<(size_t)globalDepth)-1);
}
/*
 * insert <key,value> entry in hash table
 * Split & Redistribute bucket when there is overflow and if necessary increase
 * global depth
 */
template <typename K, typename V>
void ExtendibleHash<K, V>::Insert(const K &key, const V &value) {}

template class ExtendibleHash<page_id_t, Page *>;
template class ExtendibleHash<Page *, std::list<Page *>::iterator>;
// test purpose
template class ExtendibleHash<int, std::string>;
template class ExtendibleHash<int, std::list<int>::iterator>;
template class ExtendibleHash<int, int>;
} // namespace cmudb
