#include <sys/mman.h>
#include <unistd.h>
#include <pthread.h>

#include <cstddef>
#include <cstring>
#include <cstdio>
#include <cassert>

#include <deque>
#include <array>
#include <cstdlib>
#include <limits>
#include <memory>
#include <new>
#include <string>
#include <vector>
#include <atomic>

struct Metadata {
  Metadata() : front(0), back(0) {}
  std::atomic<uint_fast32_t> front;
  std::atomic<uint_fast32_t> back;
};

static constexpr uint32_t kBadPos = static_cast<uint32_t>(-1);
struct Entry {
  Entry() : ready(false), queue_position(0), size(0) {}
  std::atomic<bool> ready; // TODO(ajwong): fence?
  uint32_t queue_position;
  uint32_t size;
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
    uint32_t back;
    uint32_t new_back;
    do {
      uint32_t front = metadata_->front;
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

  Entry* PeekFront() {
    size_t front = metadata_->front;
    size_t back = metadata_->back;

    if (front == back) {
      return nullptr;
    }

    return &queue_[front];
  }

  void PopFront() {
    size_t front = metadata_->front;
    size_t back = metadata_->back;

    assert (front != back);

    // Clear ready to preserve invariant on PushEntry that the ready bit
    // is false.
    queue_[front].ready = false;

    metadata_->front = (front + 1) % max_elements_;
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

  size_t EnqueueBlob(const char* data, size_t size) {
    uint32_t front, back, new_back, new_back_no_mod;
    do {
      front = metadata_->front;
      back = metadata_->back;
      new_back_no_mod = back + size;
      new_back = new_back_no_mod % queue_size_;
      if (back >= front) {
        if (new_back < new_back_no_mod && new_back >= front)
          return kBadPos;
      } else {
        if (new_back < new_back_no_mod || new_back_no_mod > front)
          return kBadPos;
      }

      // Reserve the space. Now it's ours.
    } while(!metadata_->back.compare_exchange_weak(back, new_back));

    // 2 stage memcpy.
    if (new_back_no_mod < queue_size_) {
      memcpy(queue_ + back , data, size);
    } else {
      int first_amt = queue_size_ - back;
      int left_over = size - first_amt;
      memcpy(queue_ + back, data, first_amt);
      memcpy(queue_, data + first_amt, left_over);
    }

    return back;
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
    Entry* entry = entry_queue_.PushEntry();
    if (!entry) {
      return false;
    }
    entry->queue_position = blob_queue_.EnqueueBlob(data, size);
    entry->size = size;
    entry->ready = true;
    // queue_position == kBadPos if the BlobQueue is out of space.
    return entry->queue_position != kBadPos;
  }

  enum Status {
    OK,
    EMPTY,
    BUFFER_TOO_SMALL,
    NOT_READY,
  };

  Status Dequeue(char* data, size_t* size) {
    Entry* entry = nullptr;
    for (;;) {
      entry = entry_queue_.PeekFront();
      if (!entry) {
        return EMPTY;
      }

      if (!entry->ready) {
        return NOT_READY;
      }

      // Skip tombstoned entries.
      if (entry->queue_position != kBadPos) {
        break;
      }
      entry_queue_.PopFront();
    }

    size_t saved_size = *size;
    *size = entry->size;
    if (entry->size > saved_size) {
      return BUFFER_TOO_SMALL;
    }

    // Okay, big enough.
    blob_queue_.Dequeue(data, entry->size);
    entry_queue_.PopFront();

    return OK;
  }

 private:
  EntryQueue entry_queue_;
  BlobQueue blob_queue_;
};

static char region[4096];

struct TestData {
  TestData(char fill, size_t size) : fill(fill), size(size) {}
  char fill;
  size_t size;
};

void FillQueue(char& fill, std::deque<TestData>& record, PCQueue& queue) {
  std::string buf;
  do {
    size_t amt = (rand() % 512) +1;
    record.push_back(TestData(fill, amt));
    buf = std::string(amt, fill);
    printf("%d[%zd], ", fill, amt);
    fill++;
  } while (queue.Enqueue(buf.data(), buf.size()));
  record.pop_back();
  printf("\n");
}

size_t DrainQueue(size_t num_to_drain, std::deque<TestData>& record, PCQueue& queue) {
  std::vector<char> out;
  size_t dequeued = 0;
  printf("Draining\n");
  for (; !record.empty() && num_to_drain > 0; num_to_drain--) {
    out.resize(record.front().size);
    size_t actual_out = record.front().size;
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
  printf("Drained %zu\n", dequeued);
  return dequeued;
}

void TestPCQueue() {
  PCQueue queue = PCQueue::Create(region, sizeof(region), true);
  std::deque<TestData> record;
  char fill = 'a';
  FillQueue(fill, record, queue);

  printf("Total In Queue %zd\n", record.size());

  size_t dequeued = DrainQueue(1, record, queue);
  assert(dequeued == 1);

  printf("Enqueuing until full\n");
  FillQueue(fill, record, queue);
  printf("Total In Queue %zd\n", record.size());

  dequeued = DrainQueue(std::numeric_limits<size_t>::max(), record, queue);
  assert(dequeued > 0);

  assert(record.empty());
}

void TestBlobQueue() {
  BlobQueue queue = BlobQueue::Create(region, sizeof(region), true);
  std::deque<TestData> record;
  char fill = 'a';
  std::string buf;
  do {
    size_t amt = (rand() % 127) +1;
    record.push_back(TestData(fill, amt));
    buf = std::string(amt, fill);
    printf("%d[%zd], ", fill, amt);
    fill++;
  } while (queue.EnqueueBlob(buf.data(), buf.size()) != kBadPos);
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

  printf("Enqueuing until full\n");
  do {
    size_t amt = (rand() % 127) +1;
    record.push_back(TestData(fill, amt));
    buf = std::string(fill, amt);
    printf("%d[%zd], ", fill, amt);
    fill++;
  } while (queue.EnqueueBlob(buf.data(), buf.size()) != kBadPos);
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
  printf("\n");

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

  entry = queue.PeekFront();
  assert(entry && entry->ready);
  printf ("Dequeued %zd\n ", entry->size);
  assert(entry->size == amts.front());
  queue.PopFront();
  // Breaks API contract to inspect, but we know we're one thread.
  assert(!entry->ready);
  amts.pop_front();

  printf("Enqueuing until full\n");
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
  while ((entry = queue.PeekFront()) != nullptr) {
    assert(entry->ready);
    printf ("%zd[%zd], ", entry->size, amts.front());
    assert(entry->size == amts.front());
    queue.PopFront();
    // Breaks API contract to inspect, but we know we're one thread.
    assert(!entry->ready);
    amts.pop_front();
    dequeued++;
  }
  printf("\n");

  printf("Total Dequeued %zd\n", dequeued);
  assert(amts.empty());
}

void TestSharedMem() {
  constexpr size_t kShmemSize = 4096;
  char buf[] = "/tmp/queue_shmem_XXXXXX";

  // Create file that will be in ram and anonymous and correctly sized.
  int fd = mkstemp(buf);
  assert(fd != -1);
  unlink(buf);
  ftruncate(fd, kShmemSize);

  // Create the shared memory.
  void* shared_memory = mmap(NULL, kShmemSize, PROT_READ |PROT_WRITE,
	 MAP_SHARED, fd, 0);
  assert (shared_memory);

  void* shared_memory2 = mmap(NULL, kShmemSize, PROT_READ |PROT_WRITE,
	 MAP_SHARED, fd, 0);
  assert (shared_memory2);
  assert (shared_memory2 != shared_memory);
  printf ("Shared memory created of size %zd at %p and %p\n",
      kShmemSize,
      shared_memory,
      shared_memory2);

  PCQueue producer = PCQueue::Create(shared_memory, kShmemSize, true);
  PCQueue consumer = PCQueue::Create(shared_memory, kShmemSize, false);

  char fill = 'a';
  std::deque<TestData> record;
  FillQueue(fill, record, producer);

  DrainQueue(2, record, consumer);

  FillQueue(fill, record, producer);
  DrainQueue(std::numeric_limits<size_t>::max(), record, consumer);
}

void TestThreadedSharedMem() {
  constexpr size_t kShmemSize = 4096;
  char buf[] = "/tmp/queue_shmem_XXXXXX";

  // Create file that will be in ram and anonymous and correctly sized.
  int fd = mkstemp(buf);
  assert(fd != -1);
  unlink(buf);
  ftruncate(fd, kShmemSize);

  // Create the shared memory.
  void* shared_memory = mmap(NULL, kShmemSize, PROT_READ |PROT_WRITE,
	 MAP_SHARED, fd, 0);
  assert (shared_memory);

  void* shared_memory2 = mmap(NULL, kShmemSize, PROT_READ |PROT_WRITE,
	 MAP_SHARED, fd, 0);
  assert (shared_memory2);
  assert (shared_memory2 != shared_memory);
  printf ("Shared memory created of size %zd at %p and %p\n",
      kShmemSize,
      shared_memory,
      shared_memory2);

  PCQueue producer = PCQueue::Create(shared_memory, kShmemSize, true);
  PCQueue consumer = PCQueue::Create(shared_memory, kShmemSize, false);

  // Create test data.
  static constexpr int kNumProducers = 5;
  static constexpr int kDataPerProducer = 10;
  char fill = 0;
  std::vector<std::vector<std::string>> raw_test_data;
  raw_test_data.resize(kNumProducers);
  for (std::vector<std::string>& test_set : raw_test_data) {
    for (int i = 0; i < kDataPerProducer; ++i) {
      test_set.emplace_back(rand() % 1024, fill++);
    }
  }
  
  // Start the threads
  std::vector<pthread_t> threads;
  threads.resize(kNumProducers);
  struct ThreadMain {
    struct ThreadData {
      const std::vector<std::string>* test_set;
      PCQueue* producer;
      int id;
    };
    static void* Main(void* ctx) {
      const ThreadData& thread_data = *reinterpret_cast<ThreadData*>(ctx);
      for (const std::string& str : *thread_data.test_set) {
        while (!thread_data.producer->Enqueue(str.data(), str.size()));
      }
      return nullptr;
    }
  };
  std::array<ThreadMain::ThreadData, kNumProducers> thread_data;
  for (int i = 0; i < threads.size(); ++i) {
    thread_data[i].test_set = &raw_test_data[i];
    thread_data[i].producer = &producer;
    thread_data[i].id = i;
    pthread_create(&threads[i], NULL, &ThreadMain::Main, &thread_data[i]);
  }

  static std::array<char, 2048> output;
  PCQueue::Status status = PCQueue::OK;
  int count = 0;
  sleep(2); // BEST SYNCHRONIZATION IN THE WORLD.
  while (status != PCQueue::EMPTY) {
    size_t actual_size = output.size();
    status = consumer.Dequeue(&output[0], &actual_size);
    if (status == PCQueue::OK) {
      count++;
    }
  }
  printf("Dequeued: %d\n", count);

  for (pthread_t th : threads) {
    int result = pthread_join(th, NULL);
    assert(result == 0);
  }
}

int main(void) {
  /*
  TestEntryQueue();
  TestBlobQueue();
  TestPCQueue();
  TestSharedMem();
  */
  TestThreadedSharedMem();
  return 0;
}
