#include <limits>
#include <memory>
#include <unordered_map>
#include <vector>

namespace med {

constexpr uint64_t INVALID_ID = std::numeric_limits<uint64_t>::max();

class Edge;

enum class ErrorType { SUCCESS = 0, INVALID, DUPLICATE, NOT_FOUND };

class Node {
public:
  class NodeCtx {};
  Node() = default;
  virtual ~Node() = default;

  bool IsValid() const { return id_ != INVALID_ID && group_id_ != INVALID_ID; }

  ErrorType Set(uint64_t id, uint64_t group_id) {
    if (id == INVALID_ID) {
      return ErrorType::INVALID;
    }
    if (group_id == INVALID_ID) {
      return ErrorType::INVALID;
    }
    id_ = id;
    group_id_ = group_id;
    return ErrorType::SUCCESS;
  }

  uint64_t ID() const { return id_; }
  ErrorType SetID(uint64_t id) {
    if (id == INVALID_ID) {
      return ErrorType::INVALID;
    }
    id_ = id;
    return ErrorType::SUCCESS;
  }
  uint64_t GroupID() const { return group_id_; }
  ErrorType SetGroupID(uint64_t group_id) {
    if (group_id == INVALID_ID) {
      return ErrorType::INVALID;
    }
    group_id_ = group_id;
    return ErrorType::SUCCESS;
  }

  size_t InDegree() const { return in_edges_.size(); }
  size_t OutDegree() const { return out_edges_.size(); }

  const std::vector<std::shared_ptr<Edge>> &InEdges() const {
    return in_edges_;
  }
  const std::vector<std::shared_ptr<Edge>> &OutEdges() const {
    return out_edges_;
  }

  void AddInEdge(const std::shared_ptr<Edge> &edge) {
    in_edges_.push_back(edge);
  }
  void AddOutEdge(const std::shared_ptr<Edge> &edge) {
    out_edges_.push_back(edge);
  }

  virtual void Run(std::shared_ptr<NodeCtx> &ctx) {}

  virtual bool EnableAsync() const { return false; }

#ifdef MED_ENABLE_ASYNC
  virtual folly::Future<int> RunAsync(std::shared_ptr<NodeCtx> ctx) {
    return folly::makeFuture<int>(0);
  }
#endif

private:
  uint64_t id_ = INVALID_ID;
  uint64_t group_id_ = 0;
  std::vector<std::shared_ptr<Edge>> in_edges_;
  std::vector<std::shared_ptr<Edge>> out_edges_;
};

class Edge {
public:
  Edge() = default;
  virtual ~Edge() = default;

  bool IsValid() const {
    return id_ != INVALID_ID && from_ != nullptr && to_ != nullptr;
  }

  ErrorType Set(uint64_t id, const std::shared_ptr<Node> &from, size_t from_key,
                const std::shared_ptr<Node> &to, size_t to_key) {
    if (id == INVALID_ID) {
      return ErrorType::INVALID;
    }
    if (!from || !from->IsValid()) {
      return ErrorType::INVALID;
    }
    if (!to || !to->IsValid()) {
      return ErrorType::INVALID;
    }
    id_ = id;
    from_ = from;
    from_key_ = from_key;
    to_ = to;
    to_key_ = to_key;
    return ErrorType::SUCCESS;
  }

  uint64_t ID() const { return id_; }
  ErrorType SetID(uint64_t id) {
    if (id == INVALID_ID) {
      return ErrorType::INVALID;
    }
    id_ = id;
    return ErrorType::SUCCESS;
  }
  const std::shared_ptr<Node> &From() const { return from_; }
  uint64_t FromIdx() const { return from_key_; }
  ErrorType SetFrom(const std::shared_ptr<Node> &from, size_t from_key) {
    if (!from) {
      return ErrorType::INVALID;
    }
    from_ = from;
    from_key_ = from_key;
    return ErrorType::SUCCESS;
  }
  const std::shared_ptr<Node> &To() const { return to_; }
  uint64_t ToIdx() const { return to_key_; }
  ErrorType SetTo(const std::shared_ptr<Node> &to, size_t to_key) {
    if (!to) {
      return ErrorType::INVALID;
    }
    to_ = to;
    to_key_ = to_key;
    return ErrorType::SUCCESS;
  }

private:
  uint64_t id_ = -1;
  std::shared_ptr<Node> from_ = nullptr;
  std::shared_ptr<Node> to_ = nullptr;
  size_t from_key_ = 0;
  size_t to_key_ = 0;
};

class Graph {
public:
  Graph() = default;
  virtual ~Graph() = default;

  bool IsValid() const {
    if (id_ == INVALID_ID) {
      return false;
    }

    if (nodes_.size() != node_map_.size()) {
      return false;
    }
    for (auto &&node : nodes_) {
      if (node == nullptr || !node->IsValid() ||
          node_map_.count(node->ID()) == 0) {
        return false;
      }
      for (auto &&edge : node->InEdges()) {
        if (edge == nullptr || edge_map_.count(edge->ID()) == 0) {
          return false;
        }
      }
      for (auto &&edge : node->OutEdges()) {
        if (edge == nullptr || edge_map_.count(edge->ID()) == 0) {
          return false;
        }
      }
    }

    if (edges_.size() != edge_map_.size()) {
      return false;
    }
    for (auto &&edge : edges_) {
      if (edge == nullptr || !edge->IsValid() ||
          edge_map_.count(edge->ID()) == 0) {
        return false;
      }
      if (node_map_.count(edge->From()->ID()) == 0) {
        return false;
      }
      if (node_map_.count(edge->To()->ID()) == 0) {
        return false;
      }
    }

    return true;
  }

  uint64_t ID() const { return id_; }
  ErrorType SetID(uint64_t id) {
    if (id == INVALID_ID) {
      return ErrorType::INVALID;
    }
    id_ = id;
    return ErrorType::SUCCESS;
  }

  const std::vector<std::shared_ptr<Node>> &Nodes() const { return nodes_; }
  const std::vector<std::shared_ptr<Edge>> &Edges() const { return edges_; }
  const std::unordered_map<uint64_t, std::shared_ptr<Node>> &NodeMap() const {
    return node_map_;
  }
  const std::unordered_map<uint64_t, std::shared_ptr<Edge>> &EdgeMap() const {
    return edge_map_;
  }
  const std::shared_ptr<Node> &GetNode(uint64_t node_id) const {
    static std::shared_ptr<Node> empty = nullptr;
    auto it = node_map_.find(node_id);
    if (it == node_map_.end()) {
      return empty;
    }
    return it->second;
  }
  const std::shared_ptr<Edge> &GetEdge(uint64_t edge_id) const {
    static std::shared_ptr<Edge> empty = nullptr;
    auto it = edge_map_.find(edge_id);
    if (it == edge_map_.end()) {
      return empty;
    }
    return it->second;
  }

  ErrorType AddNode(const std::shared_ptr<Node> &node) {
    if (node == nullptr || !node->IsValid()) {
      return ErrorType::INVALID;
    }
    if (node_map_.count(node->ID()) != 0) {
      return ErrorType::DUPLICATE;
    }
    nodes_.push_back(node);
    node_map_[node->ID()] = node;
    return ErrorType::SUCCESS;
  }

  ErrorType AddEdge(const std::shared_ptr<Edge> &edge) {
    if (edge == nullptr || !edge->IsValid()) {
      return ErrorType::INVALID;
    }
    if (edge_map_.count(edge->ID()) != 0) {
      return ErrorType::DUPLICATE;
    }
    if (node_map_.count(edge->From()->ID()) == 0) {
      return ErrorType::NOT_FOUND;
    }
    if (node_map_.count(edge->To()->ID()) == 0) {
      return ErrorType::NOT_FOUND;
    }
    edges_.push_back(edge);
    edge_map_[edge->ID()] = edge;
    return ErrorType::SUCCESS;
  }

private:
  uint64_t id_ = -1;
  std::vector<std::shared_ptr<Node>> nodes_;
  std::vector<std::shared_ptr<Edge>> edges_;

  std::unordered_map<uint64_t, std::shared_ptr<Node>> node_map_;
  std::unordered_map<uint64_t, std::shared_ptr<Edge>> edge_map_;
};

ErrorType
TopologicalSort(const Graph &graph,
                std::vector<std::shared_ptr<Node>> *topological_order) {
  if (!graph.IsValid()) {
    return ErrorType::INVALID;
  }
  if (topological_order == nullptr) {
    return ErrorType::INVALID;
  }
  topological_order->clear();
  topological_order->reserve(graph.Nodes().size());
  std::unordered_map<uint64_t, size_t> in_degree;
  for (auto &&node : graph.Nodes()) {
    in_degree[node->ID()] = node->InDegree();
  }
  std::vector<std::shared_ptr<Node>> queue;
  for (auto &&node : graph.Nodes()) {
    if (in_degree[node->ID()] == 0) {
      queue.push_back(node);
    }
  }
  while (!queue.empty()) {
    auto node = queue.back();
    queue.pop_back();
    topological_order->push_back(node);
    for (auto &&edge : node->OutEdges()) {
      auto to = edge->To();
      --in_degree[to->ID()];
      if (in_degree[to->ID()] == 0) {
        queue.push_back(to);
      }
    }
  }
  if (topological_order->size() != graph.Nodes().size()) {
    topological_order->clear();
    return ErrorType::INVALID;
  }
  return ErrorType::SUCCESS;
}

class Executor {
public:
  Executor() = default;
  virtual ~Executor() = default;
  virtual bool Prepare(const Graph &graph) {
    if (!graph.IsValid()) {
      return false;
    }
    if (TopologicalSort(graph, &nodes_) != ErrorType::SUCCESS) {
      return false;
    }
    return true;
  }
  virtual void Run() {}

private:
  std::vector<std::shared_ptr<Node>> nodes_;
};

} // namespace med
