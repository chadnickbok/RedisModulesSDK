/**
 * Simple Task Runner.
 * Waits for tasks from a blocking TaskQueue.
 */

#include <atomic>
#include <shared_ptr>

#include "TaskQueue.hpp"

class TaskWorker {
public:
  TaskWorker(std::shared_ptr<TaskQueue> task_queue);
  virtual ~TaskWorker();

  void Run();
  void Stop();

private:
  std::shared
  std::atomic<bool> should_stop;
};
