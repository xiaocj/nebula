// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/logic/LoopExecutor.h"

#include "graph/planner/plan/Logic.h"

namespace nebula {
namespace graph {

LoopExecutor::LoopExecutor(const PlanNode *node, QueryContext *qctx)
    : Executor("LoopExecutor", node, qctx) {}

folly::Future<Status> LoopExecutor::execute() {
  SCOPED_TIMER(&execTime_);

  auto *loopNode = asNode<Loop>(node());
  Expression *expr = loopNode->condition();
  LOG(ERROR) << "execute: name=" << name() << "_" << id() << ", expr=" << expr->toString();

  QueryExpressionContext ctx(ectx_);
  auto value = expr->eval(ctx);
  DCHECK(value.isBool());

  LOG(ERROR) << "  value=" << value.getBool();

  finally_ = !(value.isBool() && value.getBool());
  return finish(ResultBuilder().value(std::move(value)).iter(Iterator::Kind::kDefault).build());
}

}  // namespace graph
}  // namespace nebula
