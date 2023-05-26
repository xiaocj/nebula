// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/algo/BFSShortestPathExecutor2.h"

#include "graph/planner/plan/Algo.h"
#include <sstream>


DECLARE_int32(num_operator_threads);
namespace nebula {
namespace graph {


// static std::string hashSetToString(
//   const robin_hood::unordered_flat_set<Value, std::hash<Value>>& hashSet) {
//   std::stringstream ss;
//   for (auto it= hashSet.begin(); it != hashSet.end(); ++it) {
//     ss << *it << ", ";
//   }

//   return ss.str();
// }

static std::string resultsToString(const Result& inputs) {
    std::stringstream ss;
    for (auto it= inputs.iter(); it->valid(); it->next()) {
        auto edgeVal = it->getEdge();
        if (UNLIKELY(!edgeVal.isEdge())) {
          continue;
        }
        ss << edgeVal.getEdge().dst << ", ";
    }

    return ss.str();
}

BFSShortestPathExecutor2::DirectionData::DirectionData()
  : inputs_(Result::EmptyResult().clone()) {
}

bool BFSShortestPathExecutor2::DirectionData::valid() const {
  return !stop_ && inputs_.iterRef()->valid();
}

bool BFSShortestPathExecutor2::DirectionData::canGoDeeper() const {
  return !stop_ && !finishCurrentLayerThenStop_ && !nextStepVids_.rows.empty();
}

bool BFSShortestPathExecutor2::DirectionData::getQueryResult(const ExecutionContext *ectx,
                                                             const std::string& key) {
  if (query_) {
    inputs_ = ectx->getResult(key).clone();
    currentDepth_++;
  }
  return query_;
}

void BFSShortestPathExecutor2::DirectionData::setNextQueryVids(ExecutionContext *ectx,
                                                               const std::string& key) {
    DataSet nextStepVids(std::move(nextStepVids_));
    nextStepVids.colNames = {nebula::kVid};
    ectx->setResult(key, ResultBuilder().value(std::move(nextStepVids)).build());
    query_ = true;
}

void BFSShortestPathExecutor2::DirectionData::resetNextQueryVids(ExecutionContext *ectx,
                                                                 const std::string& key) {
    DataSet nextStepVids;
    nextStepVids.colNames = {nebula::kVid};
    ectx->setResult(key, ResultBuilder().value(std::move(nextStepVids)).build());
    query_ = false;
}

BFSShortestPathExecutor2::BFSShortestPathExecutor2(const PlanNode* node, QueryContext* qctx)
  : Executor("BFSShortestPath2", node, qctx) {
}

folly::Future<Status> BFSShortestPathExecutor2::execute() {
  // MemoryTrackerVerified
  SCOPED_TIMER(&execTime_);
  pathNode_ = asNode<BFSShortestPath>(node());
  terminateEarlyVar_ = pathNode_->terminateEarlyVar();
  LOG(ERROR) << "execute: step=" << step_;

  if (step_ == 1) {
    // record the all endpoints
    for (auto iter=ectx_->getResult(pathNode_->leftVidVar()).iter(); iter->valid(); iter->next()) {
      auto& vid = iter->getColumn(0);
      left_.visitedVids_.emplace(vid, LevelData(0));
    }

    for (auto iter=ectx_->getResult(pathNode_->rightVidVar()).iter(); iter->valid(); iter->next()) {
      auto& vid = iter->getColumn(0);
      right_.visitedVids_.emplace(vid, LevelData(0));
    }

    // check
    for (auto left=left_.visitedVids_.begin(); left != left_.visitedVids_.end(); ++left) {
      auto right = right_.visitedVids_.find(left->first);
      if (right != right_.visitedVids_.end()) {
        DataSet ds;  // TODO: empty path
        ds.colNames = pathNode_->colNames();
        Status status = finish(ResultBuilder().value(Value(std::move(ds))).build());
        ectx_->setValue(terminateEarlyVar_, true);
        return folly::makeFuture<Status>(std::move(status);
      }
    }
  }

  // read query results
  if (left_.getQueryResult(ectx_, pathNode_->leftInputVar())) {
    left_.resetNextQueryVids(ectx_, pathNode_->leftVidVar());
    sharedCurrentDepth_++;
    LOG(ERROR) << "    left: depth=" << left_.currentDepth_ << "/"
      << sharedCurrentDepth_ << ", vids=" << resultsToString(left_.inputs_);
  }
  if (right_.getQueryResult(ectx_, pathNode_->rightInputVar())) {
    right_.resetNextQueryVids(ectx_, pathNode_->rightVidVar());
    sharedCurrentDepth_++;
    LOG(ERROR) << "    right_: depth=" << right_.currentDepth_ << "/"
      << sharedCurrentDepth_ << ", vids=" << resultsToString(right_.inputs_);
  }

  try {
    memory::MemoryCheckGuard guard;

    HashSet meetVids;
    while (left_.valid() && right_.valid()) {
      goOneStep(left_, right_, false, meetVids);
      goOneStep(right_, left_, true, meetVids);
    }

    // try load next layer vertexes
    LOG(ERROR) << "    sharedFrozenDepth_=" << sharedFrozenDepth_
      << ", sharedCurrentDepth_=" << sharedCurrentDepth_
      << ", pathNode_->steps()=" << pathNode_->steps()
      << ", meetVids.empty()=" << meetVids.empty();

    if (meetVids.empty() && sharedFrozenDepth_ == 0 &&
        sharedCurrentDepth_ < static_cast<int>(pathNode_->steps())) {
      if (!left_.valid() && left_.canGoDeeper()) {
        left_.setNextQueryVids(ectx_, pathNode_->leftVidVar());
      }
      if (!right_.valid() && right_.canGoDeeper()) {
        right_.setNextQueryVids(ectx_, pathNode_->rightVidVar());
      }

      if (left_.query_ || right_.query_) {
        step_++;
        return folly::makeFuture<Status>(Status::OK());
      }
    }

    // process the left nodes
    while (left_.valid()) {
      goOneStep(left_, right_, false, meetVids);
    }
    while (right_.valid()) {
      goOneStep(right_, left_, true, meetVids);
    }

    LOG(ERROR) << "    BOTH LEFT AND RIGHT are finished";
    ectx_->setValue(terminateEarlyVar_, true);

    step_++;
    DataSet ds = doConjunct(meetVids);
    ds.colNames = pathNode_->colNames();
    Status status = finish(ResultBuilder().value(Value(std::move(ds))).build());
    return folly::makeFuture<Status>(std::move(status));
  } catch (const std::bad_alloc&) {
    return folly::makeFuture<Status>(memoryExceededStatus());
  } catch (std::exception& e) {
    return folly::makeFuture<Status>(std::runtime_error(e.what()));
  }
}

Status BFSShortestPathExecutor2::goOneStep(DirectionData& thisSide,
                                           DirectionData& otherSide,
                                           bool reverse,
                                           HashSet& meetVids) {
  Iterator* iter = thisSide.inputs_.iterRef();
  while (iter->valid()) {
    auto edgeVal = iter->getEdge();
    iter->next();
    if (UNLIKELY(!edgeVal.isEdge())) {
      return Status::OK();
    }

    auto& edge = edgeVal.getEdge();
    auto dst = edge.dst;

    auto it = thisSide.visitedVids_.find(dst);
    if (it == thisSide.visitedVids_.end()) {
      LOG(ERROR) << "      new node: reverse=" << reverse << ", dst=" << dst << ", edge=" << edge;
      thisSide.nextStepVids_.rows.emplace_back(Row({dst}));
      thisSide.visitedVids_.emplace(dst, LevelData(thisSide.currentDepth_, std::move(edge)));
      tryMeet(thisSide, otherSide, dst, meetVids);
      break;
    } else if (it->second.depth_ == thisSide.currentDepth_) {
      LOG(ERROR) << "      update node: reverse=" << reverse << ", dst=" << dst;
      it->second.edges_.emplace_back(std::move(edge));
    } else {
      LOG(ERROR) << "      discard node: reverse=" << reverse << ", dst=" << dst;
    }
  }

  return Status::OK();
}

bool BFSShortestPathExecutor2::tryMeet(DirectionData& thisSide,
                                       DirectionData& otherSide,
                                       const Value& dst,
                                       HashSet& meetVids) {
  auto otherSideHit = otherSide.visitedVids_.find(dst);
  if (otherSideHit != otherSide.visitedVids_.end()) {
    int depth = thisSide.currentDepth_ + otherSideHit->second.depth_;
    if (sharedFrozenDepth_ <= 0) {
      sharedFrozenDepth_ = depth;
    }

    if (depth <= sharedFrozenDepth_)  {
      thisSide.haveFoundSomething_ = true;
      if (depth < sharedFrozenDepth_)  {
          sharedFrozenDepth_ = depth;
          otherSide.stop_ = true;
      }

      meetVids.emplace(dst);
    }

    return true;
  } else {
    return false;
  }
}

DataSet BFSShortestPathExecutor2::doConjunct(const HashSet& meetVids) const {
  DataSet ds;
  for (auto& vid : meetVids) {
    std::vector<Path> paths = createFullPaths(vid);
    for (auto& p : paths) {
      Row row;
      row.emplace_back(std::move(p));
      ds.rows.emplace_back(std::move(row));
    }
  }
  return ds;
}

std::vector<Path> BFSShortestPathExecutor2::createFullPaths(const Value& vid) const {
  std::vector<Path> paths;

  std::vector<Path> leftPaths = createPartialPaths(left_, vid);
  std::vector<Path> rightPaths = createPartialPaths(right_, vid);
  for (auto leftIt = leftPaths.begin(); leftIt != leftPaths.end(); ++leftIt) {
    leftIt->reverse();
    Path p = *leftIt;  // TODO: optimize
    for (auto rightIt = rightPaths.begin(); rightIt != rightPaths.end(); ++rightIt) {
      p.append(*rightIt);
    }
  }

  return paths;
}

std::vector<Path> BFSShortestPathExecutor2::createPartialPaths(const DirectionData& side,
                                                               const Value& vid) const {
  std::vector<Path> result;

  // find start vertex
  auto topIt = side.visitedVids_.find(vid);
  if (topIt == side.visitedVids_.end()) {
    LOG(ERROR) << "      Hit vid not found, vid=" << vid;
    return result;
  }
  if (topIt->second.depth_ == 0) {
    return result;
  }

  // create start paths
  for (auto eIt = topIt->second.edges_.begin(); eIt != topIt->second.edges_.end(); ++topIt) {
      Path p;
      p.src.vid = vid;
      p.steps.emplace_back(Step(Vertex(eIt->src, {}), -eIt->type, eIt->name, eIt->ranking, {}));
      result.emplace_back(std::move(p));
  }

  std::vector<Path> interimPaths = std::move(result);
  for (int depth = topIt->second.depth_-1; depth >= 0; --depth) {
    for (auto pathIt = interimPaths.begin(); pathIt != interimPaths.end(); ++pathIt) {
      auto& nextVid = pathIt->steps.back().dst;
      auto it = side.visitedVids_.find(nextVid);
      if (it == side.visitedVids_.end()) {
        LOG(ERROR) << "      Hit vid not found, nextVid=" << nextVid;
        continue;
      }

      if (it->second.depth_ != depth) {
        LOG(ERROR) << "      depth mismatch=" << it->second.depth_ << ", " << depth;
      }

      for (auto eIt = it->second.edges_.begin(); eIt != it->second.edges_.end(); ++it) {
        Path p = *pathIt;
        p.steps.emplace_back(Step(Vertex(eIt->src, {}), -eIt->type, eIt->name, eIt->ranking, {}));
        result.emplace_back(std::move(p));
      }
    }

    interimPaths = std::move(result);
  }

  return result;
}

}  // namespace graph
}  // namespace nebula
