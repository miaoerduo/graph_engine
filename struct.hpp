#include <vector>
#include <memory>
#include <unordered_map>

namespace med {

class Edge;

enum class ErrorType { SUCCESS = 0, INVALID, DUPLICATED };

class Node {
public:
  Node() = default;
  Node(int id) : id_(id) {}
  virtual ~Node() = default;

  int ID() const { return id_; }
  ErrorType SetID(int id) {
    if (id < 0) {
      return ErrorType::INVALID;
    }
    id_ = id;
    return ErrorType::SUCCESS;
  }
  int GroupID() const { return group_id_; }
  ErrorType SetGroupID(int group_id) {
    if (group_id < 0) {
      return ErrorType::INVALID;
    }
    group_id_ = group_id;
    return ErrorType::SUCCESS;
  }

private:
  int id_ = -1;
  int group_id_ = 0;
  std::vector<std::shared_ptr<Edge>> in_edges_;
  std::vector<std::shared_ptr<Edge>> out_edges_;
};

class Edge {
public:
  Edge() = default;
  Edge(int id, const std::shared_ptr<Node> &from, const std::shared_ptr<Node> &to) : id_(id), from_(from), to_(to) {}
  virtual ~Edge() = default;

  int ID() const { return id_; }
  ErrorType SetID(int id) {
    if (id < 0) {
      return ErrorType::INVALID;
    }
    id_ = id;
    return ErrorType::SUCCESS;
  }
  const std::shared_ptr<Node> &From() const { return from_; }
  ErrorType SetFrom(const std::shared_ptr<Node> &from) {
    if (from == nullptr) {
      return ErrorType::INVALID;
    }
    from_ = from;
    return ErrorType::SUCCESS;
  }
  const std::shared_ptr<Node> &To() const { return to_; }
  ErrorType SetTo(const std::shared_ptr<Node> &to) {
    if (to == nullptr) {
      return ErrorType::INVALID;
    }
    to_ = to;
    return ErrorType::SUCCESS;
  }

private:
  int id_ = -1;
  std::shared_ptr<Node> from_ = nullptr;
  std::shared_ptr<Node> to_ = nullptr;
};

class Graph {
public:
  Graph() = default;
  Graph(int id) : id_(id){};
  virtual ~Graph() = default;

  int ID() const { return id_; }
  void SetID(int id) { id_ = id; }
  const std::vector<std::shared_ptr<Node>> &Nodes() const { return nodes_; }
  void SetNodes(const std::vector<std::shared_ptr<Node>> &nodes) {
    nodes_ = nodes;
  }
  bool AddNode(const std::shared_ptr<Node> &node) {
    if (node == nullptr) {
      return false;
    }
    if (node_map_.count(node->ID()) == 0) {
      return false;
    }
    nodes_.push_back(node);
    node_map_[node->ID()] = node;
    return true;
  }

  const std::vector<std::shared_ptr<Edge>> &Edges() const { return edges_; }
  void SetEdges(const std::vector<std::shared_ptr<Edge>> &edges) {
    edges_ = edges;
  }
  bool AddEdge(const std::shared_ptr<Edge> &edge) {
    if (edge == nullptr) {
      return false;
    }
    if (node_map_.count(edge.From()) == 0 || node_map_.count(edge.To()) == 0) {
      return false;
    }
    if (edge_map_.count(edge->ID()) == 0) {
      return false;
    }
    edges_.push_back(edge);
    edge_map_[edge->ID()] = edge;
    return true;
  }

private:
  int id_ = -1;
  std::vector<std::shared_ptr<Node>> nodes_;
  std::vector<std::shared_ptr<Edge>> edges_;

  std::unordered_map<int, std::shared_ptr<Node>> node_map_;
  std::unordered_map<int, std::shared_ptr<Edge>> edge_map_;
};

namespace toolkit {} // namespace toolkit

} // namespace med
