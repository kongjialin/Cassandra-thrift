#include "offline_manager.h"

#include <atomic>
#include <stddef.h>
#include <algorithm>
#include <vector>

#include "common/base/functor.h"
#include "common/base/join_functor.h"
#include "common/base/timestamp.h"
#include "random_message.h"
#include "thirdparty/boost/thread/thread.hpp"
#include "thirdparty/gflags/gflags.h"
#include "thirdparty/glog/logging.h"

DEFINE_int32(thread_count, 1, "Number of client threads");
DEFINE_int32(operation_count, 10000, "Count of operations");
DEFINE_string(operation_type, "INSERT",
             "Type of operation--INSERT, SELECT");

std::atomic<size_t> hit_count(0);

double RankLatency(double rank, double* arr, size_t size) {
  size_t index = rank * size;
  if (index > 0)
    index--;
  return arr[index];
}

void Store(OfflineManager* offline_manager, size_t loop_count,
           double* latency_arr, Functor<void>* finish) {
  Message message;
  JoinFunctor* joiner = new JoinFunctor(loop_count, finish);
  for (size_t i = 0; i < loop_count; ++i) {
    RandomMessage::GenerateMessage(&message);
    int64_t start_time = GetTimeStampInUs();
    auto store_cb = [=](bool success) {
      int64_t end_time = GetTimeStampInUs();
      double latency = (end_time - start_time) / 1000.0;
      latency_arr[i] = latency;
      joiner->Run();
    }; 
    offline_manager->Store(store_cb, message);//currently it's synchronous call
  }
}

void Retrieve(OfflineManager* offline_manager, size_t loop_count,
              double* latency_arr, Functor<void>* finish) {
  JoinFunctor* joiner = new JoinFunctor(loop_count, finish);
  for (size_t i = 0; i < loop_count; ++i) {
    std::string receiver;
    RandomMessage::GenerateString(&receiver);
    int64_t start_time = GetTimeStampInUs();
    auto retrieve_cb = [=](std::vector<Message> const& msgs) {
      int64_t end_time = GetTimeStampInUs();
      if (msgs.size())
        std::atomic_fetch_add(&hit_count,static_cast<size_t>(1));
      double latency = (end_time - start_time) / 1000.0;
      latency_arr[i] = latency;
      joiner->Run();
    };
    offline_manager->Retrieve(retrieve_cb, receiver);//synchronous
  }
}

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  google::ParseCommandLineFlags(&argc, &argv, false);

  OfflineManager* offline_manager = &OfflineManager::GetInstance();
  boost::this_thread::sleep_for(boost::chrono::seconds(10));

  size_t loop_count = FLAGS_operation_count / FLAGS_thread_count;
  std::vector<boost::thread*> threads;
  std::vector<double*> latencies;
  for (int i = 0; i < FLAGS_thread_count; ++i) {
    double* latency = new double[loop_count];
    latencies.push_back(latency);
  }

  int64_t stress_begin_time = GetTimeStampInMs();
  auto joiner = NewFunctor([=]() {
    int64_t stress_end_time = GetTimeStampInMs();
    double* latency_result = new double[FLAGS_operation_count];
    double total_latency = 0;
    for (int i = 0; i < FLAGS_thread_count; ++i) {
      for (size_t j = 0; j < loop_count; ++j) {
        latency_result[i * loop_count + j] = latencies[i][j];
        total_latency += latencies[i][j];
      }
    }
    std::sort(latency_result, latency_result + FLAGS_operation_count);
    LOG(INFO) << "Operation type: " << FLAGS_operation_type;
    LOG(INFO) << "Operation count: " << FLAGS_operation_count;
    LOG(INFO) << "Hit count: " << hit_count;
    LOG(INFO) << "Thread count: " << FLAGS_thread_count;
    LOG(INFO) << "QPS: "
        << FLAGS_operation_count * 1000 / (stress_end_time - stress_begin_time);
    LOG(INFO) << "Average latency: " 
              << total_latency / FLAGS_operation_count << " ms";
    LOG(INFO) << "Min latency: " << latency_result[0] << " ms";
    LOG(INFO) << "Max latency: " << latency_result[FLAGS_operation_count - 1]
              << " ms";
    LOG(INFO) << ".95 latency: "
              << RankLatency(0.95, latency_result, FLAGS_operation_count)
              << " ms";
    LOG(INFO) << ".99 latency: "
              << RankLatency(0.99, latency_result, FLAGS_operation_count)
              << " ms";
    LOG(INFO) << ".999 latency: "
              << RankLatency(0.999, latency_result, FLAGS_operation_count)
              << " ms";
    delete [] latency_result;
    for (int i = 0; i < latencies.size(); ++i)
      delete [] latencies[i];
  });
  Functor<void>* finish = new JoinFunctor(FLAGS_thread_count, joiner);

  void (*operation)(OfflineManager* offline_manager, size_t loop_count,
      double* latency_arr, Functor<void>* finish);
   
  if (FLAGS_operation_type == "INSERT") {
    operation = &Store;
  } else if (FLAGS_operation_type == "SELECT") {
    operation = &Retrieve;
  }

  for (int i = 0; i < FLAGS_thread_count; ++i) {
    boost::thread* request_thread = new boost::thread(operation,
        offline_manager, loop_count, latencies[i], finish);
    threads.push_back(request_thread);
  }


  // Now join all
  for (int i = 0; i < FLAGS_thread_count; ++i) {
    threads[i]->join();
    delete threads[i];
  }

  boost::this_thread::sleep_for(boost::chrono::seconds(20));
  return 0;
}


