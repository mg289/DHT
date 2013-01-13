#ifndef _WAT_DHT_SERVER_H_
#define _WAT_DHT_SERVER_H_

#include "WatDHT.h"
#include "WatID.h"
#include "WatDHTState.h"
#include <pthread.h>
#include <string>
#include <vector>
#include <map>
#include <thrift/server/TThreadedServer.h>
#include <ctime>

namespace WatDHT {
	
struct value_dur {
  std::string v;
  time_t d; 
};

enum Distance { CLOSEST,
                CLOSEST_CW,
				CLOSEST_CCW };

class WatDHTServer {
 public:
  WatDHTServer(const char* id, const char* ip, int port) throw (int);  
  ~WatDHTServer();
  
  int join(const char* ip, int port);
  void maintain(const int index);
  void find_next_hop(NodeID &next_node, const WatID joining_id, Distance distance,
		     std::vector<NodeID> neighbors, std::vector<NodeID> rtable);

  void update_neighbors(std::vector<NodeID> neighbors, const NodeID& nid);
  void update_rtable(const NodeID& nid);

  void check_neighbor(const NodeID& nid, bool &is_avail, int check_neighbor); 
  void fill_neighbors(Distance distance);
  void put(const char* ip, const int port, const std::string& key, const std::string& val, const int32_t duration);
  void get(const char* ip, const int port, std::string& _return, const std::string& key);

  // Block and wait until the server shuts down.
  int wait();
 
  // Set the RPC server once it is created in a child thread.
  void set_rpc_server(apache::thrift::server::TThreadedServer* server);
  
  const std::string& get_ipaddr() { return server_node_id.ip; }
  int get_port() { return server_node_id.port; }
  const WatID& get_id() { return wat_id; } 
  NodeID get_node_id() { return server_node_id; }

  std::vector<NodeID> get_neighbors() { return neighbors; }
  void set_neighbors(std::vector<NodeID> n) { neighbors = n; }
  std::vector<NodeID> get_rtable() { return rtable; }
  void set_rtable(std::vector<NodeID> rt) { rtable = rt; }
  std::map<std::string, value_dur> get_hash_table() { return hash_table; }
  void set_hash_table(std::map<std::string, value_dur> ht) { hash_table = ht; }
  bool get_is_migrated() { return is_migrated; }
  bool get_can_receive_requests() { return can_receive_requests; }
  WatDHTState get_wat_state() { return wat_state; }
  void set_wat_state(WatDHTState::State state) { wat_state.change_state(state); }
  void wait_for_state(WatDHTState::State state) { wat_state.wait_ge(state); }

 private:
  WatID wat_id;             // This node's ID on the DHT.
  NodeID server_node_id;    // Include the ID, IP address and port.
  apache::thrift::server::TThreadedServer* rpc_server;
  pthread_t rpc_thread;
  WatDHTState wat_state;    // Tracks the current state of the node.
  static const int num_rpc_threads = 64;
  static void* start_rpc_server(void* param);

  std::vector<NodeID> neighbors;
  std::vector<NodeID> rtable;
  std::map<std::string, value_dur> hash_table;
  bool is_migrated;
  bool can_receive_requests;
  int ping_period;
  int gn_period;
};
} // namespace WatDHT

#endif
