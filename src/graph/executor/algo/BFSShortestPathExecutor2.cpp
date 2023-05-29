// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/algo/BFSShortestPathExecutor2.h"

#include "graph/planner/plan/Algo.h"
#include <sstream>


DECLARE_int32(num_operator_threads);
namespace nebula {
namespace graph {

// static std::string resultsToString(const Result& inputs) {
//     std::stringstream ss;
//     for (auto it= inputs.iter(); it->valid(); it->next()) {
//         auto edgeVal = it->getEdge();
//         if (UNLIKELY(!edgeVal.isEdge())) {
//           continue;
//         }
//         ss << edgeVal.getEdge().dst << ",";
//     }

//     return ss.str();
// }

const std::string BFSShortestPathExecutor2::LevelData::toString() const {
  std::stringstream ss;
  ss << "depth=" << depth_ << ", edges=";
  for (const auto& e : edges_) {
    ss << e << ",";
  }

  return ss.str();
}

bool BFSShortestPathExecutor2::DirectionData::valid() const {
  return !stop_ && inputs_.iterRef()->valid();
}

bool BFSShortestPathExecutor2::DirectionData::canGoDeeper() const {
  return !stop_ && !finishCurrentLayerThenStop_ && !nextStepVids_.rows.empty();
}

std::string BFSShortestPathExecutor2::DirectionData::toString() const {
  std::stringstream ss;
  ss << "currentDepth=" << currentDepth_ << ", haveFoundSomething=" << haveFoundSomething_
    << "stop=" << stop_ << ", finishCurrentLayerThenStop" << finishCurrentLayerThenStop_
    << ", visitedVids=" << visitedVids_.size() << ", nextStepVids=" << nextStepVids_.size();

  return ss.str();
}

BFSShortestPathExecutor2::BFSShortestPathExecutor2(const PlanNode* node, QueryContext* qctx)
  : Executor("BFSShortestPath2", node, qctx) {
}

bool BFSShortestPathExecutor2::getQueryResult(bool reverse) {
  const std::string& vidVar = reverse ? pathNode_->rightVidVar() : pathNode_->leftVidVar();
  const std::string& inputVar = reverse ? pathNode_->rightInputVar() : pathNode_->leftInputVar();
  DirectionData& side = reverse? right_ : left_;

  const auto& vids = ectx_->getResult(vidVar);
  if (vids.iter()->valid()) {
    side.inputs_ = ectx_->getResult(inputVar).clone();
    side.currentDepth_++;
    sharedCurrentDepth_++;

    // reset query
    DataSet nextStepVids;
    nextStepVids.colNames = {nebula::kVid};
    ectx_->setResult(vidVar, ResultBuilder().value(std::move(nextStepVids)).build());
    return true;
  } else {
    return false;
  }
}

void BFSShortestPathExecutor2::setNextQueryVids(bool reverse) {
  const std::string& vidVar = reverse ? pathNode_->rightVidVar() : pathNode_->leftVidVar();
  DirectionData& side = reverse? right_ : left_;

  DataSet nextStepVids(std::move(side.nextStepVids_));
  nextStepVids.colNames = {nebula::kVid};
  ectx_->setResult(vidVar, ResultBuilder().value(std::move(nextStepVids)).build());
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

    // check whether starts contains ends
    std::vector<Path> paths;
    for (auto left=left_.visitedVids_.begin(); left != left_.visitedVids_.end(); ++left) {
      auto right = right_.visitedVids_.find(left->first);
      if (right != right_.visitedVids_.end()) {
        paths.emplace_back(Path(Vertex(left->first, {}), std::vector<Step>()));
      }
    }
    if (!paths.empty()) {
      DataSet ds;
      ds.colNames = pathNode_->colNames();
      for (auto& p : paths) {
        Row row;
        row.emplace_back(std::move(p));
        ds.rows.emplace_back(std::move(row));
      }

      Status status = finish(ResultBuilder().value(Value(std::move(ds))).build());
      ectx_->setValue(terminateEarlyVar_, true);
      return folly::makeFuture<Status>(std::move(status));
    }
  }

  // read query results
  getQueryResult(false);
  getQueryResult(true);

  try {
    memory::MemoryCheckGuard guard;

    HashSet meetVids;
    while (left_.valid() && right_.valid()) {
      goOneStep(left_, right_, meetVids);
      goOneStep(right_, left_, meetVids);
    }

    // try load next layer vertexes
    if (meetVids.empty() && sharedFrozenDepth_ <= 0 &&
        sharedCurrentDepth_ < static_cast<int>(pathNode_->steps())) {
      bool goDeeper = false;
      if (!left_.valid() && left_.canGoDeeper()) {
        setNextQueryVids(false);
        goDeeper = true;
      }
      if (!right_.valid() && right_.canGoDeeper()) {
        setNextQueryVids(true);
        goDeeper = true;
      }

      if (goDeeper) {
        step_++;
        return folly::makeFuture<Status>(Status::OK());
      }
    }

    // process the left nodes
    while (left_.valid()) {
      goOneStep(left_, right_, meetVids);
    }
    while (right_.valid()) {
      goOneStep(right_, left_, meetVids);
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
                                           HashSet& meetVids) {
  // LOG(ERROR) << "    sharedFrozenDepth_=" << sharedFrozenDepth_
  //   << ", sharedCurrentDepth_=" << sharedCurrentDepth_
  //   << ", thisSide.haveFoundSomething_=" << thisSide.haveFoundSomething_;
  if (sharedFrozenDepth_ > 0 && !thisSide.haveFoundSomething_ &&
      sharedCurrentDepth_ >  sharedFrozenDepth_) {
    thisSide.stop_ = true;
    return Status::OK();
  }

  Iterator* iter = thisSide.inputs_.iterRef();
  while (iter->valid()) {
    auto edgeVal = iter->getEdge();
    iter->next();
    if (UNLIKELY(!edgeVal.isEdge())) {
      continue;
    }

    auto& edge = edgeVal.getEdge();
    auto dst = edge.dst;

    auto it = thisSide.visitedVids_.find(dst);
    if (it == thisSide.visitedVids_.end()) {
      // LOG(INFO) << "      new: reverse=" << reverse << ", dst=" << dst
      //           << ", depth=" << thisSide.currentDepth_;
      thisSide.nextStepVids_.rows.emplace_back(Row({dst}));
      thisSide.visitedVids_.emplace(dst, LevelData(thisSide.currentDepth_, std::move(edge)));
      tryMeet(thisSide, otherSide, dst, meetVids);
      break;
    } else if (it->second.depth_ == thisSide.currentDepth_) {
      it->second.edges_.emplace_back(std::move(edge));
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
          // LOG(ERROR) << "    sharedFrozenDepth_=" << sharedFrozenDepth_ << ", depth=" << depth
          //   << ", thisSide.haveFoundSomething_=" << thisSide.haveFoundSomething_;
          // LOG(ERROR) << "       === other side STOP!";

          meetVids.clear();
          sharedFrozenDepth_ = depth;
          otherSide.stop_ = true;
      }

      // LOG(INFO) << "      MEET!!, dst=" << dst << ", sharedFrozenDepth_=" << sharedFrozenDepth_;
      meetVids.emplace(dst);
    }

    return true;
  } else {
    return false;
  }
}

DataSet BFSShortestPathExecutor2::doConjunct(const HashSet& meetVids) const {
  // LOG(ERROR) << "left visited:";
  // for(auto it = left_.visitedVids_.begin(); it != left_.visitedVids_.end(); ++it) {
  //   LOG(ERROR) << "      vid=" << it->first << ", data=" << it->second.toString();
  // }
  // LOG(ERROR) << "right visited:";
  // for(auto it = right_.visitedVids_.begin(); it != right_.visitedVids_.end(); ++it) {
  //   LOG(ERROR) << "      vid=" << it->first << ", data=" << it->second.toString();
  // }

  // LOG(INFO) << "meets:";
  // for (auto& vid : meetVids) {
  //   LOG(INFO) << "      vid=" << vid;
  // }

  DataSet ds;
  for (auto& vid : meetVids) {
    std::vector<Path> paths = createFullPaths(vid);
    // LOG(INFO) << "   paths:" << paths.size();

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
      paths.emplace_back(p);
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
    Path p;
    p.src.vid = vid;
    result.emplace_back(std::move(p));
    return result;
  }

  // create start paths
  for (auto eIt = topIt->second.edges_.begin(); eIt != topIt->second.edges_.end(); ++eIt) {
      Path p;
      p.src.vid = vid;
      p.steps.emplace_back(Step(Vertex(eIt->src, {}), -eIt->type, eIt->name, eIt->ranking, {}));
      result.emplace_back(std::move(p));
  }

  for (int depth = topIt->second.depth_-1; depth > 0; --depth) {
    std::vector<Path> interimPaths = std::move(result);

    // LOG(ERROR) << "    LOOP, depth=" << depth << ", interimPaths.size=" << interimPaths.size();
    // for (auto& p : interimPaths) {
    //   LOG(ERROR) << "       path=" <<p;
    // }

    for (auto pathIt = interimPaths.begin(); pathIt != interimPaths.end(); ++pathIt) {
      auto& nextVid = pathIt->steps.back().dst.vid;
      auto it = side.visitedVids_.find(nextVid);
      if (it == side.visitedVids_.end()) {
        LOG(ERROR) << "       path=" << *pathIt << ", Hit vid not found, nextVid=" << nextVid;
        continue;
      }

      if (it->second.depth_ != depth) {
        LOG(ERROR) << "      depth mismatch=" << it->second.depth_ << ", " << depth;
      }

      for (auto eIt = it->second.edges_.begin(); eIt != it->second.edges_.end(); ++eIt) {
        Path p = *pathIt;
        p.steps.emplace_back(Step(Vertex(eIt->src, {}), -eIt->type, eIt->name, eIt->ranking, {}));
        result.emplace_back(std::move(p));
      }
    }
  }

  return result;
}

}  // namespace graph
}  // namespace nebula
