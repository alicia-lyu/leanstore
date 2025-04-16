#pragma once
#include <cstdlib>
#include "BTreeGeneric.hpp"
#include "BTreeIteratorInterface.hpp"
#include "Exceptions.hpp"
#include "leanstore/storage/buffer-manager/BufferFrame.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
using namespace leanstore::storage;
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
namespace btree
{
// -------------------------------------------------------------------------------------
// Iterator
class BTreePessimisticIterator : public BTreePessimisticIteratorInterface
{
   friend class BTreeGeneric;

  public:
   BTreeGeneric& btree;
   const LATCH_FALLBACK_MODE mode;
   // -------------------------------------------------------------------------------------
   // Hooks
   std::function<void(HybridPageGuard<BTreeNode>& leaf)> exit_leaf_cb = nullptr;   // Optimistic mode
   std::function<void(HybridPageGuard<BTreeNode>& leaf)> enter_leaf_cb = nullptr;  // Shared at least
   std::function<void()> cleanup_cb = nullptr;
   // -------------------------------------------------------------------------------------
   s32 cur = -1;                        // Reset after every leaf change
   bool prefix_copied = false;          // Reset after every leaf change
   HybridPageGuard<BTreeNode> leaf;     // Reset after every leaf change
   HybridPageGuard<BTreeNode> p_guard;  // Reset after every leaf change
   s32 leaf_pos_in_parent = -1;         // Reset after every leaf change
   bool shift_to_right_on_frozen_swips = true;
   // -------------------------------------------------------------------------------------
   u8 buffer[PAGE_SIZE];  // Used to copy key at cur and for upper_fence/lower_fence
   u16 fence_length = 0;
   bool is_using_upper_fence;
   // -------------------------------------------------------------------------------------
   virtual ~BTreePessimisticIterator() = default;

  protected:
   template <LATCH_FALLBACK_MODE mode = LATCH_FALLBACK_MODE::SHARED>
   void TraceLeafAndLatch(const u8* key, u16 key_length)
   {
      jumpmuTry()
      {
         // exit_leaf_cb(leaf);
         leaf.unlock();
         p_guard.unlock();
         p_guard = HybridPageGuard<BTreeNode>(btree.meta_node_bf);
         leaf = HybridPageGuard<BTreeNode>(p_guard, p_guard->upper);
         leaf_pos_in_parent = 0;
         // -------------------------------------------------------------------------------------
         u16 volatile level = 0;
         // -------------------------------------------------------------------------------------
         while (!leaf->is_leaf) {
            WorkerCounters::myCounters().dt_inner_page[btree.dt_id]++;
            Swip<BTreeNode>* c_swip = nullptr;
            p_guard = std::move(leaf);
            leaf_pos_in_parent = p_guard->lowerBound<false>(key, key_length);
            if (leaf_pos_in_parent == p_guard->count) {
               c_swip = &p_guard->upper;
            } else {
               c_swip = &p_guard->getChild(leaf_pos_in_parent);
            }
            leaf = HybridPageGuard(p_guard, *c_swip);
            level = level + 1;
         }
         // -------------------------------------------------------------------------------------
         if (mode == LATCH_FALLBACK_MODE::EXCLUSIVE) {
            leaf.toExclusive();
         } else {
            leaf.toShared();
         }
         assert(leaf->is_leaf);
         prefix_copied = false;
         if (enter_leaf_cb) {
            enter_leaf_cb(leaf);
         }
         jumpmu_return;
      }
      jumpmuCatch() {}
   }
   // -------------------------------------------------------------------------------------
   void gotoPage(const Slice& key)
   {
      COUNTERS_BLOCK()
      {
         if (mode == LATCH_FALLBACK_MODE::EXCLUSIVE) {
            WorkerCounters::myCounters().dt_goto_page_exec[btree.dt_id]++;
         } else {
            WorkerCounters::myCounters().dt_goto_page_shared[btree.dt_id]++;
         }
      }
      std::cout << std::hex << std::setw(2) << std::setfill('0') << "\rgotoPage: ";
      for (size_t i = 0; i < key.length(); i++) {
         std::cout << (int)key.data()[i] << " ";
      }
      // -------------------------------------------------------------------------------------
      // TODO: refactor when we get ride of serializability tests
      if (mode == LATCH_FALLBACK_MODE::SHARED) {
         this->TraceLeafAndLatch<LATCH_FALLBACK_MODE::SHARED>(key.data(), key.length());
      } else if (mode == LATCH_FALLBACK_MODE::EXCLUSIVE) {
         this->TraceLeafAndLatch<LATCH_FALLBACK_MODE::EXCLUSIVE>(key.data(), key.length());
      } else {
         UNREACHABLE();
      }
   }

   void turnPage(int diff)  // +1 next page, -1 prev page
   {
      jumpmuTry()
      {
         std::cout << "turnPage by " << diff << ".";
         assert(diff == -1 || diff == 1);
         // exit_leaf_cb(leaf);
         leaf.unlock();
         p_guard.unlock();
         auto* start_page = &leaf;
         while (true) {
            auto parent_swip_handler = BTreeGeneric::findParent<true>(btree, *start_page->bf);
            auto pos_in_parent = parent_swip_handler.pos;
            if (pos_in_parent == -2) {
               // We are at the root, keep the original page, cur == leaf->count - 1
               break;
            }
            auto next_pos_in_parent = pos_in_parent + diff;
            p_guard = parent_swip_handler.getParentReadPageGuard<BTreeNode>();
            p_guard.unlock();
            COUNTERS_BLOCK()
            {
               WorkerCounters::myCounters().dt_inner_page[btree.dt_id]++;
            }
            leaf.recheck();
            if (next_pos_in_parent > p_guard->count) {
               start_page = &p_guard;
            } else if (next_pos_in_parent == p_guard->count) {
               leaf = HybridPageGuard<BTreeNode>(p_guard, p_guard->upper);
               leaf_pos_in_parent = pos_in_parent;
               break;
            } else {
               leaf = HybridPageGuard<BTreeNode>(p_guard, p_guard->getChild(pos_in_parent + 1));
               leaf_pos_in_parent = pos_in_parent;
               break;
            }
         }

         while (!leaf->is_leaf) {  // go to the smallest page in this subtree
            COUNTERS_BLOCK()
            {
               WorkerCounters::myCounters().dt_inner_page[btree.dt_id]++;
            }
            p_guard.recheck();
            p_guard = std::move(leaf);
            assert(p_guard->count > 0);
            leaf.recheck();
            leaf = HybridPageGuard<BTreeNode>(p_guard, p_guard->getChild(0));
            leaf_pos_in_parent = 0;
         }

         if (mode == LATCH_FALLBACK_MODE::EXCLUSIVE) {
            leaf.toExclusive();
         } else {
            leaf.toShared();
         }
         prefix_copied = false;
         if (enter_leaf_cb) {
            enter_leaf_cb(leaf);
         }
         jumpmu_return;
      }
      jumpmuCatch() {}
   }

   // -------------------------------------------------------------------------------------
  public:
   BTreePessimisticIterator(BTreeGeneric& btree, const LATCH_FALLBACK_MODE mode = LATCH_FALLBACK_MODE::SHARED) : btree(btree), mode(mode) {}
   // -------------------------------------------------------------------------------------
   void enterLeafCallback(std::function<void(HybridPageGuard<BTreeNode>& leaf)> cb) { enter_leaf_cb = cb; }
   void exitLeafCallback(std::function<void(HybridPageGuard<BTreeNode>& leaf)> cb) { exit_leaf_cb = cb; }
   void cleanUpCallback(std::function<void()> cb) { cleanup_cb = cb; }
   // -------------------------------------------------------------------------------------
   OP_RESULT seekExactWithHint(Slice key, bool higher = true)  // EXP
   {
      if (cur == -1) {
         return seekExact(key);
      }
      cur = leaf->linearSearchWithBias<true>(key.data(), key.length(), cur, higher);
      if (cur == -1) {
         return seekExact(key);
      } else {
         return OP_RESULT::OK;
      }
   }
   // -------------------------------------------------------------------------------------
   // ==
   virtual OP_RESULT seekExact(Slice key) override
   {
      if (cur == -1 || !keyInCurrentBoundaries(key)) {
         gotoPage(key);
      }
      cur = leaf->lowerBound<true>(key.data(), key.length());
      if (cur != -1) {
         return OP_RESULT::OK;
      } else {
         return OP_RESULT::NOT_FOUND;
      }
   }
   // -------------------------------------------------------------------------------------
   // >=
   virtual OP_RESULT seek(Slice key) override
   {
      if (cur == -1 || leaf->compareKeyWithBoundaries(key.data(), key.length()) != 0) {
         gotoPage(key);
      }
      cur = leaf->lowerBound<false>(key.data(), key.length());
      if (cur < leaf->count) {
         return OP_RESULT::OK;
      } else {
         // TODO: Is there a better solution?
         // In composed keys {K1, K2}, it can happen that when we look for {2, 0} we always land on {1,..} page because its upper bound is beyond
         // {2,0} Example: TPC-C Neworder
         return next();
      }
   }
   // -------------------------------------------------------------------------------------
   // <=
   virtual OP_RESULT seekForPrev(Slice key) override
   {
      if (cur == -1 || leaf->compareKeyWithBoundaries(key.data(), key.length()) != 0) {
         gotoPage(key);
      }
      bool is_equal = false;
      cur = leaf->lowerBound<false>(key.data(), key.length(), &is_equal);
      if (is_equal == true) {
         return OP_RESULT::OK;
      } else if (cur == 0) {
         return prev();
      } else {
         cur -= 1;
         return OP_RESULT::OK;
      }
   }
   // -------------------------------------------------------------------------------------

   void gotoMinPage()
   {
      u8 const zero_char[] = {u8'\0'};
      auto min_key = Slice(zero_char, 1);
      gotoPage(min_key);
   }

   void gotoMaxPage()
   {
      u8 const max_chars[EFFECTIVE_PAGE_SIZE - 1] = {u8'\0'};
      auto max_key = Slice(max_chars, EFFECTIVE_PAGE_SIZE - 1);
      gotoPage(max_key);
   }

   virtual OP_RESULT next() override
   {
      COUNTERS_BLOCK()
      {
         WorkerCounters::myCounters().dt_next_tuple[btree.dt_id]++;
      }
      // Check if the iterator is at its initial state
      if (cur == -1) {
         gotoMinPage();
         cur = 0;
         return OP_RESULT::OK;
      }
      while (true) {
         ensure(leaf.guard.state != GUARD_STATE::OPTIMISTIC);
         if (cur + 1 < leaf->count) {
            cur += 1;
            std::cout << "Inner-page next()" << std::endl;
            return OP_RESULT::OK;
         } else if (leaf->upper_fence.length == 0) {
            std::cout << "end next()" << std::endl;
            return OP_RESULT::NOT_FOUND;
         } else {
            fence_length = leaf->upper_fence.length + 1;
            is_using_upper_fence = true;
            std::memcpy(buffer, leaf->getUpperFenceKey(), leaf->upper_fence.length);
            buffer[fence_length - 1] = 0;
            // -------------------------------------------------------------------------------------
            if (exit_leaf_cb) {
               exit_leaf_cb(leaf);
               exit_leaf_cb = nullptr;
            }
            p_guard.unlock();
            leaf.unlock();
            // -------------------------------------------------------------------------------------
            if (cleanup_cb) {
               cleanup_cb();
               cleanup_cb = nullptr;
            }
            // -------------------------------------------------------------------------------------
            if (leaf_pos_in_parent != -1 && leaf_pos_in_parent + 1 <= p_guard->count) {
               jumpmuTry()
               {
                  s32 next_leaf_pos = leaf_pos_in_parent + 1;
                  Swip<BTreeNode>& c_swip = (next_leaf_pos < p_guard->count) ? p_guard->getChild(next_leaf_pos) : p_guard->upper;
                  HybridPageGuard next_leaf(p_guard, c_swip);
                  p_guard.unlock();
                  leaf.unlock();
                  leaf = std::move(next_leaf);
                  leaf_pos_in_parent = next_leaf_pos;
                  cur = 0;
                  if (mode == LATCH_FALLBACK_MODE::EXCLUSIVE) {
                     leaf.toExclusive();
                  } else {
                     leaf.toShared();
                  }
                  std::cout << "\rnext leaf: leaf_pos_in_parent = " << leaf_pos_in_parent << "/" << p_guard->count;
                  prefix_copied = false;
                  // -------------------------------------------------------------------------------------
                  if (enter_leaf_cb) {
                     enter_leaf_cb(leaf);
                  }
                  // -------------------------------------------------------------------------------------
                  if (leaf->count == 0) {
                     jumpmu_continue;
                  }
                  ensure(cur < leaf->count);
                  COUNTERS_BLOCK()
                  {
                     WorkerCounters::myCounters().dt_next_tuple_opt[btree.dt_id]++;
                  }
                  jumpmu_return OP_RESULT::OK;
               }
               jumpmuCatch() {}
            }
            // Construct the next key (lower bound)
            turnPage(1);
            // -------------------------------------------------------------------------------------
            if (leaf->count == 0) {
               cleanUpCallback([&, to_find = leaf.bf]() {
                  jumpmuTry()
                  {
                     btree.tryMerge(*to_find, true);
                  }
                  jumpmuCatch() {}
               });
               COUNTERS_BLOCK()
               {
                  WorkerCounters::myCounters().dt_empty_leaf[btree.dt_id]++;
               }
               continue;
            }
            cur = leaf->lowerBound<false>(buffer, fence_length);
            if (cur == leaf->count) {
               continue;
            }
            return OP_RESULT::OK;
         }
      }
   }
   // -------------------------------------------------------------------------------------
   virtual OP_RESULT prev() override
   {
      COUNTERS_BLOCK()
      {
         WorkerCounters::myCounters().dt_prev_tuple[btree.dt_id]++;
      }
      // -------------------------------------------------------------------------------------
      // Check if the iterator is at its initial state
      if (cur == -1) {
         gotoMaxPage();
         cur = leaf->count - 1;
         return OP_RESULT::OK;
      }
      while (true) {
         ensure(leaf.guard.state != GUARD_STATE::OPTIMISTIC);
         if (cur - 1 >= 0) {
            cur -= 1;
            std::cout << "Inner-page prev()" << std::endl;
            return OP_RESULT::OK;
         } else if (leaf->lower_fence.length == 0) {
            std::cout << "end prev()" << std::endl;
            return OP_RESULT::NOT_FOUND;
         } else {
            fence_length = leaf->lower_fence.length;
            is_using_upper_fence = false;
            std::memcpy(buffer, leaf->getLowerFenceKey(), fence_length);
            // -------------------------------------------------------------------------------------
            if (exit_leaf_cb) {
               exit_leaf_cb(leaf);
               exit_leaf_cb = nullptr;
            }
            p_guard.unlock();
            leaf.unlock();
            // -------------------------------------------------------------------------------------
            if (cleanup_cb) {
               cleanup_cb();
               cleanup_cb = nullptr;
            }
            // -------------------------------------------------------------------------------------
            if (leaf_pos_in_parent != -1 && leaf_pos_in_parent - 1 >= 0) {
               jumpmuTry()
               {
                  s32 next_leaf_pos = leaf_pos_in_parent - 1;
                  Swip<BTreeNode>& c_swip = p_guard->getChild(next_leaf_pos);
                  HybridPageGuard next_leaf(p_guard, c_swip, LATCH_FALLBACK_MODE::JUMP);
                  p_guard.unlock();
                  leaf.unlock();
                  leaf = std::move(next_leaf);
                  leaf_pos_in_parent = next_leaf_pos;
                  cur = leaf->count - 1;
                  if (mode == LATCH_FALLBACK_MODE::EXCLUSIVE) {
                     leaf.toExclusive();
                  } else {
                     leaf.toShared();
                  }
                  std::cout << "prev leaf: leaf_pos_in_parent = " << leaf_pos_in_parent << "/" << p_guard->count << std::endl;
                  prefix_copied = false;
                  // -------------------------------------------------------------------------------------
                  if (enter_leaf_cb) {
                     enter_leaf_cb(leaf);
                  }
                  // -------------------------------------------------------------------------------------
                  if (leaf->count == 0) {
                     jumpmu_continue;
                  }
                  COUNTERS_BLOCK()
                  {
                     WorkerCounters::myCounters().dt_prev_tuple_opt[btree.dt_id]++;
                  }
                  jumpmu_return OP_RESULT::OK;
               }
               jumpmuCatch() {}
            }
            // Construct the next key (lower bound)
            turnPage(-1);
            // -------------------------------------------------------------------------------------
            if (leaf->count == 0) {
               COUNTERS_BLOCK()
               {
                  WorkerCounters::myCounters().dt_empty_leaf[btree.dt_id]++;
               }
               continue;
            }
            bool is_equal = false;
            cur = leaf->lowerBound<false>(buffer, fence_length, &is_equal);
            if (is_equal) {
               return OP_RESULT::OK;
            } else if (cur > 0) {
               cur -= 1;
            } else {
               continue;
            }
         }
      }
   }
   // -------------------------------------------------------------------------------------
   virtual void assembleKey()
   {
      if (!prefix_copied) {
         leaf->copyPrefix(buffer);
         prefix_copied = true;
      }
      leaf->copyKeyWithoutPrefix(cur, buffer + leaf->prefix_length);
   }
   virtual Slice key() override { return Slice(buffer, leaf->getFullKeyLen(cur)); }
   virtual MutableSlice mutableKeyInBuffer() { return MutableSlice(buffer, leaf->getFullKeyLen(cur)); }
   virtual MutableSlice mutableKeyInBuffer(u16 size)
   {
      assert(size < PAGE_SIZE);
      return MutableSlice(buffer, size);
   }
   // -------------------------------------------------------------------------------------
   virtual bool isKeyEqualTo(Slice other) override
   {
      ensure(false);
      return other == key();
   }
   virtual Slice keyPrefix() override { return Slice(leaf->getPrefix(), leaf->prefix_length); }
   virtual Slice keyWithoutPrefix() override { return Slice(leaf->getKey(cur), leaf->getKeyLen(cur)); }
   virtual u16 valueLength() { return leaf->getPayloadLength(cur); }
   virtual Slice value() override { return Slice(leaf->getPayload(cur), leaf->getPayloadLength(cur)); }
   // -------------------------------------------------------------------------------------
   virtual bool keyInCurrentBoundaries(Slice key) { return leaf->compareKeyWithBoundaries(key.data(), key.length()) == 0; }
   // -------------------------------------------------------------------------------------
   bool isValid() { return cur != -1; }
   bool isLastOne()
   {
      assert(isValid());
      assert(cur != leaf->count);
      return (cur + 1) == leaf->count;
   }
   void reset()
   {
      leaf.unlock();
      cur = -1;
      leaf_pos_in_parent = -1;
      prefix_copied = false;
   }
};
// -------------------------------------------------------------------------------------
class BTreeSharedIterator : public BTreePessimisticIterator
{
  public:
   BTreeSharedIterator(BTreeGeneric& btree, const LATCH_FALLBACK_MODE mode = LATCH_FALLBACK_MODE::SHARED) : BTreePessimisticIterator(btree, mode) {}
};
// -------------------------------------------------------------------------------------
class BTreeExclusiveIterator : public BTreePessimisticIterator
{
  private:
  public:
   BTreeExclusiveIterator(BTreeGeneric& btree) : BTreePessimisticIterator(btree, LATCH_FALLBACK_MODE::EXCLUSIVE) {}
   BTreeExclusiveIterator(BTreeGeneric& btree, BufferFrame* bf, const u64 bf_version)
       : BTreePessimisticIterator(btree, LATCH_FALLBACK_MODE::EXCLUSIVE)
   {
      Guard as_it_was_witnessed(bf->header.latch, bf_version);
      as_it_was_witnessed.recheck();
      leaf = HybridPageGuard<BTreeNode>(std::move(as_it_was_witnessed), bf);
      leaf.toExclusive();
   }
   // -------------------------------------------------------------------------------------
   void markAsDirty() { leaf.markAsDirty(); }
   virtual OP_RESULT seekToInsertWithHint(Slice key, bool higher = true)
   {
      ensure(cur != -1);
      cur = leaf->linearSearchWithBias(key.data(), key.length(), cur, higher);
      if (cur == -1) {
         return seekToInsert(key);
      } else {
         return OP_RESULT::OK;
      }
   }
   virtual OP_RESULT seekToInsert(Slice key)
   {
      if (cur == -1 || !keyInCurrentBoundaries(key)) {
         gotoPage(key);
      }
      bool is_equal = false;
      cur = leaf->lowerBound<false>(key.data(), key.length(), &is_equal);
      if (is_equal) {
         return OP_RESULT::DUPLICATE;
      } else { // if cur == leaf->count, it will be split
         return OP_RESULT::OK;
      }
   }
   virtual OP_RESULT enoughSpaceInCurrentNode(const u16 key_length, const u16 value_length)
   {
      return (leaf->canInsert(key_length, value_length)) ? OP_RESULT::OK : OP_RESULT::NOT_ENOUGH_SPACE;
   }
   virtual OP_RESULT enoughSpaceInCurrentNode(Slice key, const u16 value_length)
   {
      return (leaf->canInsert(key.length(), value_length)) ? OP_RESULT::OK : OP_RESULT::NOT_ENOUGH_SPACE;
   }
   virtual void insertInCurrentNode(Slice key, u16 value_length)
   {
      assert(keyInCurrentBoundaries(key));
      ensure(enoughSpaceInCurrentNode(key, value_length) == OP_RESULT::OK);
      cur = leaf->insertDoNotCopyPayload(key.data(), key.length(), value_length, cur);
   }
   virtual void insertInCurrentNode(Slice key, Slice value)
   {
      assert(keyInCurrentBoundaries(key));
      assert(enoughSpaceInCurrentNode(key, value.length()) == OP_RESULT::OK);
      assert(cur != -1);
      cur = leaf->insertDoNotCopyPayload(key.data(), key.length(), value.length(), cur);
      std::memcpy(leaf->getPayload(cur), value.data(), value.length());
   }
   virtual void splitForKey(Slice key)
   {
      while (true) {
         jumpmuTry()
         {
            if (cur == -1 || !keyInCurrentBoundaries(key)) {
               btree.findLeafCanJump<LATCH_FALLBACK_MODE::SHARED>(leaf, key.data(), key.length());
            }
            BufferFrame* bf = leaf.bf;
            leaf.unlock();
            cur = -1;
            // -------------------------------------------------------------------------------------
            btree.trySplit(*bf);
            jumpmu_break;
         }
         jumpmuCatch() {}
      }
   }
   virtual OP_RESULT insertKV(Slice key, Slice value)
   {
      OP_RESULT ret;
   restart: {
      ret = seekToInsert(key);
      if (ret != OP_RESULT::OK) {
         return ret;
      }
      assert(keyInCurrentBoundaries(key));
      ret = enoughSpaceInCurrentNode(key, value.length());
      if (ret == OP_RESULT::NOT_ENOUGH_SPACE) {
         splitForKey(key);
         goto restart;
      } else if (ret == OP_RESULT::OK) {
         insertInCurrentNode(key, value);
         return OP_RESULT::OK;
      } else {
         return ret;
      }
   }
   }
   // -------------------------------------------------------------------------------------
   virtual OP_RESULT replaceKV(Slice, Slice)
   {
      ensure(false);
      return OP_RESULT::NOT_FOUND;
   }
   // -------------------------------------------------------------------------------------
   // The caller must retain the payload when using any of the following payload resize functions
   virtual void shorten(const u16 new_size) { leaf->shortenPayload(cur, new_size); }
   // -------------------------------------------------------------------------------------
   bool extendPayload(const u16 new_length)
   {
      if (new_length >= EFFECTIVE_PAGE_SIZE) {
         return false;
      }
      ensure(cur != -1 && new_length > leaf->getPayloadLength(cur));
      OP_RESULT ret;
      while (!leaf->canExtendPayload(cur, new_length)) {
         if (leaf->count == 1) {
            return false;
         }
         assembleKey();
         Slice key = this->key();
         splitForKey(key);
         ret = seekExact(key);
         ensure(ret == OP_RESULT::OK);
      }
      assert(cur != -1);
      leaf->extendPayload(cur, new_length);
      return true;
   }
   // -------------------------------------------------------------------------------------
   virtual MutableSlice mutableValue() { return MutableSlice(leaf->getPayload(cur), leaf->getPayloadLength(cur)); }
   // -------------------------------------------------------------------------------------
   virtual void contentionSplit()
   {
      if (!FLAGS_contention_split) {
         return;
      }
      const u64 random_number = utils::RandomGenerator::getRandU64();
      if ((random_number & ((1ull << FLAGS_cm_update_on) - 1)) == 0) {
         s64 last_modified_pos = leaf.bf->header.contention_tracker.last_modified_pos;
         leaf.bf->header.contention_tracker.last_modified_pos = cur;
         leaf.bf->header.contention_tracker.restarts_counter += leaf.hasFacedContention();
         leaf.bf->header.contention_tracker.access_counter++;
         if ((random_number & ((1ull << FLAGS_cm_period) - 1)) == 0) {
            const u64 current_restarts_counter = leaf.bf->header.contention_tracker.restarts_counter;
            const u64 current_access_counter = leaf.bf->header.contention_tracker.access_counter;
            const u64 normalized_restarts = 100.0 * current_restarts_counter / current_access_counter;
            leaf.bf->header.contention_tracker.restarts_counter = 0;
            leaf.bf->header.contention_tracker.access_counter = 0;
            // -------------------------------------------------------------------------------------
            if (last_modified_pos != cur && normalized_restarts >= FLAGS_cm_slowpath_threshold && leaf->count > 2) {
               s16 split_pos = std::min<s16>(last_modified_pos, cur);
               leaf.unlock();
               cur = -1;
               jumpmuTry()
               {
                  btree.trySplit(*leaf.bf, split_pos);
                  WorkerCounters::myCounters().contention_split_succ_counter[btree.dt_id]++;
               }
               jumpmuCatch()
               {
                  WorkerCounters::myCounters().contention_split_fail_counter[btree.dt_id]++;
               }
            }
         }
      }
   }
   // -------------------------------------------------------------------------------------
   virtual OP_RESULT removeCurrent()
   {
      if (!(leaf.bf != nullptr && cur >= 0 && cur < leaf->count)) {
         ensure(false);
         return OP_RESULT::OTHER;
      } else {
         leaf->removeSlot(cur);
         return OP_RESULT::OK;
      }
   }
   virtual OP_RESULT removeKV(Slice key)
   {
      auto ret = seekExact(key);
      if (ret == OP_RESULT::OK) {
         leaf->removeSlot(cur);
         return OP_RESULT::OK;
      } else {
         return ret;
      }
   }
   // -------------------------------------------------------------------------------------
   // Returns true if it tried to merge
   bool mergeIfNeeded()
   {
      if (leaf->freeSpaceAfterCompaction() >= BTreeNodeHeader::underFullSize) {
         leaf.unlock();
         cur = -1;
         jumpmuTry()
         {
            btree.tryMerge(*leaf.bf);
         }
         jumpmuCatch()
         {
            // nothing, it is fine not to merge
         }
         return true;
      } else {
         return false;
      }
   }
};
// -------------------------------------------------------------------------------------
}  // namespace btree
}  // namespace storage
}  // namespace leanstore
