#include "WatDHTServer.h"
#include "WatDHT.h"
#include "WatID.h"
#include "WatDHTState.h"
#include "WatDHTHandler.h"
#include <cstddef>
#include <string.h>
#include <pthread.h>
#include <protocol/TBinaryProtocol.h>
#include <server/TSimpleServer.h>
#include <transport/TSocket.h>
#include <transport/TServerSocket.h>
#include <transport/TBufferTransports.h>
#include <thrift/concurrency/ThreadManager.h>
#include <thrift/concurrency/PosixThreadFactory.h>
#include <thrift/server/TThreadedServer.h>
#include <stdio.h>
#include <iostream>


using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;
using namespace ::apache::thrift::concurrency;

using boost::shared_ptr;

namespace WatDHT {

WatDHTServer::WatDHTServer(const char* id, 
                           const char* ip, 
                           int port) throw (int) : rpc_server(NULL) {
  wat_id.set_using_md5(id);
  wat_id.debug_md5();
  server_node_id.id = wat_id.to_string();
  server_node_id.ip = ip;
  server_node_id.port = port;

  int table_size = 4;
  NodeID null_id; 
  for(int i = 0; i < table_size; i++) {
    neighbors.push_back(null_id); 
    rtable.push_back(null_id); 
  }

  ping_period = 30;
  gn_period = 10;
  int rc = pthread_create(&rpc_thread, NULL, start_rpc_server, this);
  if (rc != 0) {
    throw rc;
  }
}

WatDHTServer::~WatDHTServer() {
  delete rpc_server;
}

// Join the DHT network and wait
int WatDHTServer::join(const char* ip, int port) {
  wat_state.wait_ge(WatDHTState::SERVER_CREATED);
  std::vector<NodeID> nodes;
  std::map<std::string, std::string> keys;
  
  boost::shared_ptr<TSocket> socket(new TSocket(ip, port));
  boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
  boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
  try {
    WatDHTClient client(protocol);
    transport->open();    
    // Send a join request to its bootstrap node
    client.join(nodes, server_node_id);

    transport->close();

    if(nodes.size() == 0) return -1;

    // Update neighbors and rtable using returned nodes
    NodeID null_id;
    NodeID cr_neighbor1 = null_id;
    NodeID cr_neighbor2 = null_id;
    NodeID ccr_neighbor1 = null_id;
    NodeID ccr_neighbor2 = null_id;
    
    WatID server_WatID;
    server_WatID.copy_from(server_node_id.id);

    int count = 1;
    for(unsigned int i =0; i < nodes.size(); i++){
      //clockwise neighbors
      WatID node_WatID;
      node_WatID.copy_from(nodes[i].id);
      WatID cn1;
      WatID cn2;
      cn1.copy_from(cr_neighbor1.id);
      cn2.copy_from(cr_neighbor2.id);
      
      //counter-clockwise neighbors
      WatID ccn1;
      WatID ccn2;
      ccn1.copy_from(ccr_neighbor1.id);
      ccn2.copy_from(ccr_neighbor2.id);
      
      //update N-set
      if(node_WatID != server_WatID && nodes[i].port != 0){
	    update_neighbors(neighbors, nodes[i]);
	    update_rtable(nodes[i]);
        count++;
      }
    }
  } catch (TTransportException e) {
    printf("Exception calling join!\n");
    printf("Caught exception: %s\n", e.what());
  }

  bool migration_in_progress = true;
  int mkv_timeout = 1;
  socket.reset(new TSocket(nodes[0].ip, nodes[0].port));
  transport.reset(new TBufferedTransport(socket));
  protocol.reset(new TBinaryProtocol(transport));
  WatDHTClient client(protocol);

  while(migration_in_progress) {
    try {
      transport->open();    
      // Issue migrate_kv to predecessor
      std::map<std::string, std::string> hash;
      client.migrate_kv(hash, server_node_id.id);
      transport->close();
      std::map<std::string, std::string>::iterator it = hash.begin();
      while(it != hash.end()) {
        value_dur val;
	    time_t duration = -1;
	    val.v = it->second;
	    val.d = duration;
	    hash_table[it->first] = val;
      }		
      migration_in_progress = false;
    } catch (TTransportException e) {
      printf("Transport exception calling migrate_kv\n");
      printf("Caught exception: %s\n", e.what());
      migration_in_progress = false;
    } catch (WatDHTException e) {
      WatDHTErrorType error_type;
      if(e.error_code == error_type.OL_MIGRATION_IN_PROGRESS) {
        printf("Caught exception: %s\n", e.what());
	    usleep(mkv_timeout*1000);
	    mkv_timeout *= 2;
      } else if(e.error_code == error_type.INCORRECT_MIGRATION_SOURCE) {
        printf("INCORRECT_MIGRATION_SOURCE\n");
        printf("Caught exception: %s\n", e.what());
	    socket.reset(new TSocket(e.node.ip, e.node.port));
	    transport.reset(new TBufferedTransport(socket));
	    protocol.reset(new TBinaryProtocol(transport));
	    WatDHTClient client(protocol);
      }
    }
  }

  // Call gossip_neighbors to each of the nodes in neighbor set
  for(unsigned int i = 0; i < neighbors.size(); i++) {
    if(neighbors[i].port == 0) continue;
    socket.reset(new TSocket(neighbors[i].ip, neighbors[i].port));
    transport.reset(new TBufferedTransport(socket));
    protocol.reset(new TBinaryProtocol(transport));
    WatDHTClient client(protocol);
    try {  
      transport->open();
      std::vector<NodeID> result;
      client.gossip_neighbors(result, server_node_id, neighbors);
      for(unsigned int i = 0; i < result.size(); i++) {
        if(result[i].port == 0) continue;
	    update_neighbors(neighbors, result[i]);
      }
      transport->close();
    } catch (TTransportException e) {
      printf("Exception calling gossip_neighbors\n");
      printf("Caught exception: %s\n", e.what());
    }
  }  

  // For each region without an entry in the routing table, 
  // issue a maintain request for the last point in that region
  for(unsigned int i = 0; i < rtable.size(); i++) {
    if(rtable[i].port == 0) {
      maintain(i);
    }
  }
  return 0;
}

void WatDHTServer::maintain(const int index) {
  std::string rt_entry = "";
  for(int i = 0; i < 128; i++) {
    if(i < index) {
      rt_entry += wat_id.get_kth_bin(i, 1); 
    } else {
      rt_entry += 1;
    }
  }
  WatID rt_entry_id;
  rt_entry_id.copy_from(rt_entry);
  NodeID next_node;
  
  // Try closest b/c closest_ccw may not check if it is equal
  find_next_hop(next_node, rt_entry_id, CLOSEST, neighbors, rtable);
  // Check if closest is in the right region
  WatID next_id;
  next_id.copy_from(next_node.id);
  if(next_id.hmatch_bin(get_id(), 1) != index - 1) {
    find_next_hop(next_node, rt_entry_id, CLOSEST_CCW, neighbors, rtable);
	next_id.copy_from(next_node.id);
	if(next_id.hmatch_bin(get_id(), 1) != index - 1) {
          rtable[index].port = 0;
	  return; // no node in region
	}
  }
  
  if(next_node == server_node_id) return;
  boost::shared_ptr<TSocket> socket(new TSocket(next_node.ip, next_node.port));
  boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
  boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
  WatDHTClient client(protocol);
  try {
    transport->open();
    std::vector<NodeID> result;
    client.maintain(result, rt_entry, server_node_id); 
    rtable[index] = result[0];
    transport->close();
  } catch (TTransportException e) {
    printf("Exception calling maintain\n");
    printf("Caught exception: %s\n", e.what());
  }
}

void WatDHTServer::find_next_hop(NodeID &next_node, const WatID joining_id, Distance distance, 
				  std::vector<NodeID> neighbors, std::vector<NodeID> rtable) {
  std::vector<NodeID> nodes;
  nodes.insert(nodes.end(), neighbors.begin(), neighbors.end());
  nodes.insert(nodes.end(), rtable.begin(), rtable.end());
  WatID min_distance;
  if(distance == CLOSEST) {
    min_distance = joining_id.distance(wat_id);
  } else if(distance == CLOSEST_CW) {
    min_distance = joining_id.distance_cr(wat_id);
  } else if(distance == CLOSEST_CCW) {
    min_distance = joining_id.distance_ccr(wat_id);
  }
  next_node = server_node_id;
  for(unsigned int i = 0; i < nodes.size(); i++) {
    if(nodes[i].port == 0) continue;
    WatID curr_id, curr_distance;
    curr_id.copy_from(nodes[i].id);
    if(distance == CLOSEST) {
      curr_distance = joining_id.distance(curr_id);
    } else if(distance == CLOSEST_CW) {
      curr_distance = joining_id.distance_cr(curr_id);
    } else if(distance == CLOSEST_CCW) {
      curr_distance = joining_id.distance_ccr(curr_id);
    }	
    if(curr_distance < min_distance) {
      min_distance = curr_distance;
      next_node = nodes[i];
    }
  }
}

void WatDHTServer::update_neighbors(std::vector<NodeID> neighbor_set, 
				    const NodeID& nid) {
				  
  if(nid == server_node_id) return;
  // Update neighbors, if necessary
  WatID caller_id, caller_distance;
  WatID neighbor_id, neighbor_distance;
  std::string id = nid.id;
  caller_id.copy_from(id);
  neighbor_id.copy_from(neighbor_set[0].id);
  caller_distance = wat_id.distance_ccr(caller_id);
  neighbor_distance = wat_id.distance_ccr(neighbor_id);
  if(neighbor_set[0].port == 0 || caller_distance < neighbor_distance) {
    neighbor_id.copy_from(neighbor_set[1].id);
    neighbor_distance = wat_id.distance_ccr(neighbor_id);
    if(neighbor_set[1].port == 0 || caller_distance < neighbor_distance) {
      neighbor_set[0] = neighbor_set[1];
      neighbor_set[1] = nid;
    } else if(caller_distance != neighbor_distance) {
      neighbor_set[0] = nid;
    }
  }
  
  neighbor_id.copy_from(neighbor_set[3].id);
  caller_distance = wat_id.distance_cr(caller_id);
  neighbor_distance = wat_id.distance_cr(neighbor_id);
  if(neighbor_set[3].port == 0 || caller_distance < neighbor_distance) {
    neighbor_id.copy_from(neighbor_set[2].id);
    neighbor_distance = wat_id.distance_cr(neighbor_id);
    if(neighbor_set[2].port == 0 || caller_distance < neighbor_distance) {
      neighbor_set[3] = neighbor_set[2];
      neighbor_set[2] = nid;
    } else if(caller_distance != neighbor_distance) {
      neighbor_set[3] = nid;
    }
  }
  neighbors = neighbor_set;
}

void WatDHTServer::update_rtable(const NodeID& nid) {
  WatID node_WatID;
  node_WatID.copy_from(nid.id);
  int index;
  WatID server_WatID;
  server_WatID.copy_from(server_node_id.id);
  index = node_WatID.hmatch_bin(server_WatID, 1);
  if(index < 3){
    rtable[index+1] = nid;
  }
}

void WatDHTServer::check_neighbor(const NodeID& nid, bool &is_avail, int index) {
  try {
    boost::shared_ptr<TSocket> socket(new TSocket(nid.ip, nid.port));
    boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
    boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
    WatDHTClient client(protocol);
    transport->open();
    std::vector<NodeID> result;
    client.gossip_neighbors(result, server_node_id, neighbors);
    for(unsigned int i = 0; i < result.size(); i++) {
      if(result[i].port == 0) continue;
      update_neighbors(neighbors, result[i]);
    }
    transport->close();
    is_avail = true;
  } catch (TTransportException e) {
    printf("Transport exception calling gossip during maintenance\n");
    printf("Caught exception: %s\n", e.what());
    neighbors[index].port = 0;
  }
}

void WatDHTServer::fill_neighbors(Distance distance) {
  NodeID next_node;
  find_next_hop(next_node, wat_id, distance, neighbors, rtable);
  try {
    boost::shared_ptr<TSocket> socket(new TSocket(next_node.ip, next_node.port));
    boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
    boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
    WatDHTClient client(protocol);
    transport->open();
    NodeID result;
    if(distance == CLOSEST_CCW) {
      client.closest_node_ccr(result, server_node_id.id);
      neighbors[1] = result;
    } else if(distance == CLOSEST_CW) {
      client.closest_node_cr(result, server_node_id.id);
      neighbors[2] = result;
    }
    transport->close();
  } catch (TTransportException e) {
    printf("Transport exception calling gossip during maintenance\n");
    printf("Caught exception: %s\n", e.what());
  }
}

// Used to test get
void WatDHTServer::get(const char* ip, const int port, std::string& _return, const std::string& key) {
  try {
    boost::shared_ptr<TSocket> socket(new TSocket(ip, port));
    boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
    boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
    WatDHTClient client(protocol);
    transport->open();
    client.get(_return, key);
    transport->close();
  } catch (TTransportException e) {
    printf("Transport exception calling gossip during maintenance\n");
    printf("Caught exception: %s\n", e.what());
  }
}

// Used to test put
void WatDHTServer::put(const char* ip, const int port, const std::string& key, const std::string& val, const int32_t duration) {
  try {
    boost::shared_ptr<TSocket> socket(new TSocket(ip, port));
    boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
    boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
    WatDHTClient client(protocol);
    transport->open();
    client.put(key, val, duration);
    transport->close();
  } catch (TTransportException e) {
    printf("Transport exception calling put\n");
    printf("Caught exception: %s\n", e.what());
  }
}

int WatDHTServer::wait() {
  wat_state.wait_ge(WatDHTState::SERVER_CREATED);
  int time_to_ping = ping_period;
  while(true) {
    sleep(gn_period);
	time_to_ping -= gn_period;
    // Call gn on all neighbors
    std::vector<NodeID> result;
    bool is_avail_ccn = false;
    bool is_avail_cn = false;
    for(unsigned int i = 0; i < 2; i++) {
      if(neighbors[i].port == 0) continue;
    }

    for(unsigned int i = 2; i < 4; i++) {
      if(neighbors[i].port == 0) continue;
    }

    if(time_to_ping <= 0) {
	  time_to_ping = ping_period;
      for(unsigned int i = 0; i < rtable.size(); i++) {
        if(rtable[i].port == 0) {
	      maintain(i);
	      continue;
        }
        boost::shared_ptr<TSocket> socket(new TSocket(rtable[i].ip, rtable[i].port));
        boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
        boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
        WatDHTClient client(protocol);
        try {
	      transport->open();
	      std::string remote_str;
	      client.ping(remote_str);
	      transport->close();
        } catch (TTransportException e) {
	      printf("Transport exception calling ping during maintenance\n");
	      printf("Caught exception: %s\n", e.what());
	      maintain(i);
        }
      }
	}
  }
  pthread_join(rpc_thread, NULL);
  return 0;
}

void WatDHTServer::set_rpc_server(TThreadedServer* server) {
  rpc_server = server;
  wat_state.change_state(WatDHTState::SERVER_CREATED);
}

void* WatDHTServer::start_rpc_server(void* param) {
  WatDHTServer* dht = static_cast<WatDHTServer*>(param);
  shared_ptr<WatDHTHandler> handler(new WatDHTHandler(dht));
  shared_ptr<TProcessor> processor(new WatDHTProcessor(handler)); 
  shared_ptr<TServerTransport> serverTransport(
     new TServerSocket(dht->get_port()));
  shared_ptr<TTransportFactory> transportFactory(
      new TBufferedTransportFactory());
  shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());
  shared_ptr<ThreadManager> threadManager = 
      ThreadManager::newSimpleThreadManager(num_rpc_threads, 0);
  shared_ptr<PosixThreadFactory> threadFactory = 
      shared_ptr<PosixThreadFactory>(new PosixThreadFactory());
  threadManager->threadFactory(threadFactory);
  threadManager->start();
  TThreadedServer* server = new TThreadedServer(
      processor, serverTransport, transportFactory, protocolFactory);
  dht->set_rpc_server(server);
  server->serve();
  return NULL;
}
} // namespace WatDHT

using namespace WatDHT;

int main(int argc, char **argv) {
  if (argc < 4) {
    printf("Usage: %s server_id ip_address port [ip_address port]\n", argv[0]);
    return -1;
  }
  try {
    // Create the DHT node with the given IP address and port.    
    WatDHTServer server(argv[1], argv[2], atoi(argv[3]));    
    // Join the DHT ring via the bootstrap node.  
    if (argc >= 6 && server.join(argv[4], atoi(argv[5])) == -1) {
        printf("Unable to connect to join network, exiting\n"); 
        return -1;
    }
    server.wait(); // Wait until server shutdown.
  } catch (int rc) {
    printf("Caught exception %d, exiting\n", rc);
    return -1;
  }
  return 0;
}