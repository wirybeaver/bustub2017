/**
 * LRU implementation
 */
#include "buffer/lru_replacer.h"
#include "page/page.h"

namespace cmudb {

    template<typename T>
    LRUReplacer<T>::LRUReplacer() {}

    template<typename T>
    LRUReplacer<T>::~LRUReplacer() {}

/*
 * Insert value into LRU
 */
    template<typename T>
    void LRUReplacer<T>::Insert(const T &value) {
        std::lock_guard<std::mutex> lck(latch);
        auto mp_iter = mp.find(value);
        if (mp_iter == mp.end()) {
            lst.emplace_front(value);
            mp.emplace(value, lst.begin());
        } else {
            lst.splice(lst.begin(), lst, mp_iter->second);
            mp[value] = lst.begin();
        }
    }

/* If LRU is non-empty, pop the head member from LRU to argument "value", and
 * return true. If LRU is empty, return false
 */
    template<typename T>
    bool LRUReplacer<T>::Victim(T &value) {
        std::lock_guard<std::mutex> lck(latch);
        if (mp.empty()) {
            return false;
        }
        value = lst.back();
        lst.pop_back();
        mp.erase(value);
        return true;
    }

/*
 * Remove value from LRU. If removal is successful, return true, otherwise
 * return false
 */
    template<typename T>
    bool LRUReplacer<T>::Erase(const T &value) {
        std::lock_guard<std::mutex> lck(latch);
        auto mp_iter = mp.find(value);
        if (mp_iter == mp.end()) {
            return false;
        }
        auto lst_iter = mp_iter->second;
        mp.erase(mp_iter);
        lst.erase(lst_iter);
        return true;
    }

    template<typename T>
    size_t LRUReplacer<T>::Size() {
        std::lock_guard<std::mutex> lck(latch);
        return mp.size();
    }

    template
    class LRUReplacer<Page *>;

// test only
    template
    class LRUReplacer<int>;

} // namespace cmudb
