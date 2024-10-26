#include "config.h"
#include <list>
#include <algorithm>

/**
 * Replacer is an abstract class that tracks page usage.
 */
class Replacer {
 public:
  Replacer() = default;
  virtual ~Replacer() = default;

  /**
   * Remove the victim frame as defined by the replacement policy.
   * @param[out] frame_id id of frame that was removed, nullptr if no victim was found
   * @return true if a victim frame was found, false otherwise
   */
  virtual auto Victim(frame_id_t *frame_id) -> bool = 0;


  /** @return the number of elements in the replacer that can be victimized */
  // virtual auto Size() -> size_t = 0;
};

class LRUReplacer : public Replacer {
 public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit LRUReplacer(size_t num_pages);

  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override;

  void RecordAccess(const frame_id_t *frame_id);

  auto Victim(frame_id_t *frame_id) -> bool override;


 private:
  std::list<frame_id_t> LRU_list_;
  frame_id_t maximum_pages_;
};
