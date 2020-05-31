#include <list>

#include "hash/extendible_hash.h"
#include "page/page.h"

namespace cmudb {

/*
 * constructor
 * array_size: fixed array size for each bucket
 */
    template <typename K, typename V>
    ExtendibleHash<K, V>::ExtendibleHash(size_t size) {
        this->bucketSize = size;
    }

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
        unique_lock<mutex> globalLock(latch);
        int pos = getIdx(key);
        unique_lock<mutex> localLock(buckets[pos]->latch);
        globalLock.unlock();
        int count = buckets[pos]->mp.erase(key);
        return count>0;
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
    void ExtendibleHash<K, V>::Insert(const K &key, const V &value) {
        unique_lock<mutex> globalLock(latch);
        shared_ptr<Bucket> cur = buckets[getIdx(key)];
        while(cur->mp.count(key)==0 && cur->mp.size()>= bucketSize) {
            auto mask = 1u<<(unsigned)(cur->localDepth);
            cur->localDepth++;
            if(cur->localDepth > globalDepth) {
                globalDepth++;
                int length = buckets.size();
                for(int i=0; i<length; i++) {
                    buckets.push_back(buckets[i]);
                }
            }
            bucketNum++;
            auto newBucketPtr = make_shared<Bucket>(cur->localDepth);
            for(auto iter =cur->mp.begin(); iter!=cur->mp.end();) {
                if(HashKey(iter->first) & mask) {
                    newBucketPtr->mp[iter->first] = iter->second;
                } else {
                    iter++;
                }
            }
            for(size_t i=0; i<buckets.size(); i++) {
                if(buckets[i]== cur && (i & mask)) {
                    buckets[i] = newBucketPtr;
                }
            }
            cur = buckets[getIdx(key)];
        }
        cur->mp[key] = value;
    }

    template class ExtendibleHash<page_id_t, Page *>;
    template class ExtendibleHash<Page *, std::list<Page *>::iterator>;
// test purpose
    template class ExtendibleHash<int, std::string>;
    template class ExtendibleHash<int, std::list<int>::iterator>;
    template class ExtendibleHash<int, int>;
} // namespace cmudb
