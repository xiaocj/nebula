// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_ALGO_BFSSHORTESTPATHEXECUTOR_H_
#define GRAPH_EXECUTOR_ALGO_BFSSHORTESTPATHEXECUTOR_H_
#include <robin_hood.h>

#include "graph/executor/Executor.h"

// BFSShortestPath has two inputs.  GetNeighbors(From) & GetNeighbors(To)
// There are two Main functions
// First : Get the next vid for GetNeighbors to expand
// Second: Extract edges from GetNeighbors to form path, concatenate the path(From) and the path(To)
//         into a complete path
//
//
// Functions:
// `buildPath`: extract edges from GetNeighbors put it into allLeftEdges or allRightEdges
//   and set the vid that needs to be expanded in the next step
//
// `conjunctPath`: concatenate the path(From) and the path(To) into a complete path
//   allLeftEdges needs to match the previous step of the allRightEdges
//   then current step of the allRightEdges each time
//   Eg. a->b->c->d
//   firstStep:  allLeftEdges [<b, a->b>]  allRightEdges [<c, d<-c>],   can't find common vid
//   secondStep: allLeftEdges [<b, a->b>, <c, b->c>] allRightEdges [<b, c<-b>, <c, d<-c>]
//   we should use allLeftEdges(secondStep) to match allRightEdges(firstStep) first
//   if find common vid, no need to match allRightEdges(secondStep)
//
// Member:
// `allLeftEdges_` : is a array, each element in the array is a hashTable
//  hash table
//    KEY   : the VID of the vertex
//    VALUE : edges visited at the current step (the destination is KEY)
//
// `allRightEdges_` : same as allLeftEdges_
//
// `leftVisitedVids_` : keep already visited vid to avoid repeated visits (left)
// `rightVisitedVids_` : keep already visited vid to avoid repeated visits (right)
// `currentDs_`: keep the paths matched in current step
namespace nebula {
namespace graph {
class BFSShortestPath;
class BFSShortestPathExecutor2 final : public Executor {
 private:
  class LevelData {
  public:
    explicit LevelData(int depth): depth_(depth) {}
    explicit LevelData(int depth, const Edge&& edge): depth_(depth) { edges_.emplace_back(edge); }

    const int depth_;
    std::vector<Edge> edges_;
  };

  class DirectionData {
  public:
    DirectionData();

    bool valid() const;
    bool canGoDeeper() const;

    bool getQueryResult(const ExecutionContext *ectx, const std::string& key);
    void setNextQueryVids(ExecutionContext *ectx, const std::string& key);
    void resetNextQueryVids(ExecutionContext *ectx, const std::string& key);

    bool query_{true};
    Result inputs_;

    int currentDepth_{0};
    std::unordered_map<Value, LevelData> visitedVids_;
    DataSet nextStepVids_;
    bool haveFoundSomething_;

    bool stop_{false};
    bool finishCurrentLayerThenStop_{false};
  };

 public:
  using HashSet = robin_hood::unordered_flat_set<Value, std::hash<Value>>;
  BFSShortestPathExecutor2(const PlanNode* node, QueryContext* qctx);
//      : Executor("BFSShortestPath2", node, qctx) {}

  folly::Future<Status> execute() override;

 private:
  Status goOneStep(DirectionData& sideA, DirectionData& sideB, bool reverse, HashSet& meetVids);
  bool tryMeet(DirectionData& sideA, DirectionData& sideB, const Value& dst, HashSet& meetVids);

  DataSet doConjunct(const std::vector<Value>& meetVids, bool oddStep) const;

  DataSet doConjunct(const HashSet& meetVids) const;
  std::vector<Path> createFullPaths(const Value& vid) const;
  std::vector<Path> createPartialPaths(const DirectionData& side, const Value& vid) const;

 private:
  const BFSShortestPath* pathNode_{nullptr};
  size_t step_{1};

  // cache data shared for both sides
  int sharedCurrentDepth_{0};
  int sharedFrozenDepth_{0};

  // cache data for each side
  DirectionData left_;
  DirectionData right_;

  std::string terminateEarlyVar_;
};

}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_EXECUTOR_ALGO_BFSSHORTESTPATHEXECUTOR_H_
