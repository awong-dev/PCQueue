#include <cstddef>
#include <cstring>
#include <cstdio>
#include <cassert>

#include <deque>
#include <cstdlib>
#include <memory>
#include <new>
#include <string>
#include <vector>
#include <atomic>

struct Metadata {
  Metadata() : front(0), back(0) {}
  std::atomic<size_t> front;
  std::atomic<size_t> back;
};

struct Entry {
  Entry() : ready(false), size(0) {}
  std::atomic<bool> ready; // TODO(ajwong): fence?
  size_t size;
};

class EntryQueue {
 public:
  EntryQueue(Metadata* metadata, Entry* queue, size_t max_elements)
    : metadata_(metadata),
      max_elements_(max_elements),
      queue_(queue) {
  }

  static EntryQueue Create(void* region_start, size_t region_size, bool initialize) {
    size_t size = (region_size - sizeof(Metadata)) / sizeof(Entry);
    Metadata* metadata = reinterpret_cast<Metadata*>(region_start);
    Entry* queue = reinterpret_cast<Entry*>(static_cast<char*>(region_start) + sizeof(Metadata));
    if (initialize) {
      new (region_start) Metadata();
      new (queue) Entry[size];
    }
    return EntryQueue(metadata, queue, size);
  }

  Entry* PushEntry() {
    size_t back;
    size_t new_back;
    do {
      size_t front = metadata_->front;
      back = metadata_->back;

      new_back = (back + 1) % max_elements_;
      if (new_back == front) {
          return nullptr;
      }

      // Commit the entry.
    } while(!metadata_->back.compare_exchange_weak(back, new_back));

    // Ready should be cleared on creation and on each PopEntry() call
    // keeping newly allocated instances initialized.
    assert(queue_[back].ready == false);

    return &queue_[back];
  }

  Entry* PeekBack() {
    size_t front = metadata_->front;
    size_t back = metadata_->back;

    if (front == back) {
      return nullptr;
    }

    return &queue_[front];
  }

  void PopBack() {
    size_t front = metadata_->front;
    size_t back = metadata_->back;

    assert (front != back);

    // Clear ready to preserve invariant on PushEntry that the ready bit
    // is false.
    queue_[front].ready = false;

    metadata_->front = (metadata_->front + 1) % max_elements_;
  }

 private:
  Metadata* metadata_;
  size_t max_elements_;
  Entry* queue_;
};

class BlobQueue {
 public:
  BlobQueue(Metadata* metadata, char* queue, size_t size)
    : metadata_(metadata),
      queue_size_(size),
      queue_(queue) {
  }

  static BlobQueue Create(void* region_start, size_t region_size, bool initialize) {
    size_t size = region_size - sizeof(Metadata);
    Metadata* metadata = reinterpret_cast<Metadata*>(region_start);
    char* queue = static_cast<char*>(region_start) + sizeof(Metadata);
    if (initialize) {
      new (region_start) Metadata();
      new (queue) char[size];
    }
    return BlobQueue(metadata, queue, size);
  }

  bool Enqueue(const char* data, size_t size) {
    size_t front, back, new_back, new_back_no_mod;
    do {
      front = metadata_->front;
      back = metadata_->back;
      new_back_no_mod = back + size;
      new_back = new_back_no_mod % queue_size_;
      if (back >= front) {
        if (new_back < new_back_no_mod && new_back >= front)
          return false;
      } else {
        if (new_back < new_back_no_mod || new_back_no_mod > front)
          return false;
      }

      // Reserve the space. Now it's ours.
    } while(!metadata_->back.compare_exchange_weak(back, new_back));

    // 2 stage memcpy.
    if (new_back_no_mod < queue_size_) {
      memcpy(queue_ + back , data, size);
    } else {
      int left_over = queue_size_ - back;
      int first_amt = size - left_over;
      memcpy(queue_ + back, data, first_amt);
      memcpy(queue_, data + first_amt, left_over);
    }

    return true;
  }

  // It's assumed that an external structure is tracking size.
  void Dequeue(char* data, size_t size) {
    // Advance the queue.
    size_t front = metadata_->front;
    size_t new_front_no_mod = front + size;
    if (new_front_no_mod < queue_size_) {
      memcpy(data, queue_ + front, size);
    } else {
      int first_amt = queue_size_ - front;
      int left_over = size - first_amt;
      memcpy(data, queue_ + front, first_amt);
      memcpy(data + first_amt, queue_, left_over);
    }

    metadata_->front = new_front_no_mod % queue_size_;
  }

 private:
  Metadata* metadata_;
  size_t queue_size_;
  char* queue_;
};

class PCQueue {
 public:
  PCQueue(EntryQueue entry_queue, BlobQueue blob_queue)
      : entry_queue_(entry_queue), blob_queue_(blob_queue) {
  }

  static PCQueue Create(void* region_start, size_t region_size, bool initialize) {
    constexpr size_t kEntryQueueSize = 1000;
    return PCQueue(EntryQueue::Create(region_start, kEntryQueueSize, initialize),
                   BlobQueue::Create(static_cast<char*>(region_start) + kEntryQueueSize,
                                     region_size - kEntryQueueSize, initialize));
  }

  bool Enqueue(const char* data, size_t size) {
    Entry* entry = nullptr;
    // Okay to spin because consumer will eventually wake up.
    while ((entry = entry_queue_.PushEntry()) == nullptr);
    entry->size = size;
    if (!blob_queue_.Enqueue(data, size))
      return false;
    entry->ready = true;
    return true;
  }

  enum Status {
    OK,
    EMPTY,
    BUFFER_TOO_SMALL,
    NOT_READY,
  };

  Status Dequeue(char* data, size_t* size) {
    size_t saved_size = *size;
    Entry* entry = entry_queue_.PeekBack();
    if (!entry) {
      return EMPTY;
    }

    if (!entry->ready) {
      return NOT_READY;
    }

    *size = entry->size;
    if (*size > saved_size) {
      return BUFFER_TOO_SMALL;
    }

    // Okay, big enough.
    blob_queue_.Dequeue(data, *size);

    entry_queue_.PopBack();
    return OK;
  }

 private:
  EntryQueue entry_queue_;
  BlobQueue blob_queue_;
};

static char region[4096];

void TestPCQueue() {
  PCQueue queue = PCQueue::Create(region, sizeof(region), true);
  struct Entry {
    Entry(char fill, size_t size) : fill(fill), size(size) {}
    char fill;
    size_t size;
  };
  std::deque<Entry> record;
  char fill = 'a';
  std::string buf;
  do {
    size_t amt = (rand() % 127) +1;
    record.push_back(Entry(fill, amt));
    buf = std::string(amt, fill);
    printf("%d[%zd], ", fill, amt);
    fill++;
  } while (queue.Enqueue(buf.data(), buf.size()));
  record.pop_back();
  printf("\n");

  printf("Total In Queue %zd\n", record.size());

  std::vector<char> out;
  out.resize(record.front().size);
  size_t actual_out = record.front().size;
  PCQueue::Status status = queue.Dequeue(&out[0], &actual_out);
  assert(status == PCQueue::OK);
  assert(actual_out == record.front().size);
  printf ("Dequeued %d, %zd\n ", out[0], out.size());
  for (char ch : out) {
    assert(ch == record.front().fill);
  }
  record.pop_front();

  do {
    size_t amt = (rand() % 127) +1;
    record.push_back(Entry(fill, amt));
    buf = std::string(fill, amt);
    printf("%d[%zd], ", fill, amt);
    fill++;
  } while (queue.Enqueue(buf.data(), buf.size()));
  record.pop_back();
  printf("\n");
  printf("Total In Queue %zd\n", record.size());

  printf("Popping: ");

  size_t dequeued = 0;
  while (!record.empty()) {
    actual_out = record.front().size;
    out.resize(record.front().size);
    PCQueue::Status status = queue.Dequeue(&out[0], &actual_out);
    assert(status == PCQueue::OK);
    assert(actual_out == record.front().size);
    printf ("%d[%zd], ", out[0], out.size());
    for (char ch : out) {
      assert(ch == record.front().fill);
    }
    dequeued++;
    record.pop_front();
  }

  printf("Total Dequeued %zd\n", dequeued);
  assert(record.empty());
}

void TestBlobQueue() {
  BlobQueue queue = BlobQueue::Create(region, sizeof(region), true);
  struct TestData {
    TestData(char fill, size_t size) : fill(fill), size(size) {}
    char fill;
    size_t size;
  };
  std::deque<TestData> record;
  char fill = 'a';
  std::string buf;
  do {
    size_t amt = (rand() % 127) +1;
    record.push_back(TestData(fill, amt));
    buf = std::string(amt, fill);
    printf("%d[%zd], ", fill, amt);
    fill++;
  } while (queue.Enqueue(buf.data(), buf.size()));
  record.pop_back();
  printf("\n");

  printf("Total In Queue %zd\n", record.size());

  std::vector<char> out;
  out.resize(record.front().size);
  queue.Dequeue(&out[0], record.front().size);
  printf ("Dequeued %d, %zd\n ", out[0], out.size());
  for (char ch : out) {
    assert(ch == record.front().fill);
  }
  record.pop_front();

  do {
    size_t amt = (rand() % 127) +1;
    record.push_back(TestData(fill, amt));
    buf = std::string(fill, amt);
    printf("%d[%zd], ", fill, amt);
    fill++;
  } while (queue.Enqueue(buf.data(), buf.size()));
  record.pop_back();
  printf("\n");
  printf("Total In Queue %zd\n", record.size());

  printf("Popping: ");

  size_t dequeued = 0;
  while (!record.empty()) {
    out.resize(record.front().size);
    queue.Dequeue(&out[0], record.front().size);
    printf ("%d[%zd], ", out[0], out.size());
    for (char ch : out) {
      assert(ch == record.front().fill);
    }
    dequeued++;
    record.pop_front();
  }

  printf("Total Dequeued %zd\n", dequeued);
  assert(record.empty());
}

void TestEntryQueue() {
  EntryQueue queue = EntryQueue::Create(region, sizeof(region), true);
  std::deque<size_t> amts;
  Entry* entry = nullptr;
  while ((entry = queue.PushEntry()) != nullptr) {
    size_t amt = rand();
    entry->size = amt;
    amts.push_back(amt);
    printf ("%zd, ", amts.back());
    entry->ready = true;
  }
  printf("\n");

  printf("Total In Queue %zd\n", amts.size());

  entry = queue.PeekBack();
  assert(entry && entry->ready);
  printf ("Dequeued %zd\n ", entry->size);
  assert(entry->size == amts.front());
  queue.PopBack();
  // Breaks API contract to inspect, but we know we're one thread.
  assert(!entry->ready);
  amts.pop_front();

  while ((entry = queue.PushEntry()) != nullptr) {
    size_t amt = rand();
    entry->size = amt;
    amts.push_back(amt);
    printf ("%zd, ", amts.back());
    entry->ready = true;
  }
  printf("\n");

  printf("Total In Queue %zd\n", amts.size());

  printf("Popping: ");
  size_t dequeued = 0;
  while ((entry = queue.PeekBack()) != nullptr) {
    assert(entry->ready);
    printf ("%zd[%zd], ", entry->size, amts.front());
    assert(entry->size == amts.front());
    queue.PopBack();
    // Breaks API contract to inspect, but we know we're one thread.
    assert(!entry->ready);
    amts.pop_front();
    dequeued++;
  }
  printf("\n");

  printf("Total Dequeued %zd\n", dequeued);
  assert(amts.empty());
}

int main(void) {
  TestEntryQueue();
//  TestBlobQueue();
//  TestPCQueue();
  return 0;
}
