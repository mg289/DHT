#include "WatDHTHandler.h"
#include <protocol/TBinaryProtocol.h>
#include <server/TSimpleServer.h>
#include <transport/TSocket.h>
#include <transport/TServerSocket.h>
#include <transport/TBufferTransports.h>
#include <boost/thread/thread.hpp>
#include <openssl/md5.h>
#include <ctime>
#include <iostream>

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;

namespace WatDHT {

WatDHTHandler::WatDHTHandler(WatDHTServer* dht_server) : server(dht_server) {
}

WatDHTHandler::~WatDHTHandler() {}

void WatDHTHandler::get(std::string& _return, const std::string& key) {
  server->get_wat_state().wait_ge(WatDHTState::SERVER_MIGRATED);
  
  NodeID next_node;
  WatID key_id;
  key_id.set_using_md5(key);
  server->find_next_hop(next_node, key_id, CLOSEST_CCW, server->get_neighbors(), server->get_rtable());
  if(server->get_node_id() == next_node) {
	std::map<std::string, value_dur> ht;
	ht = server->get_hash_table();
    std::map<std::string, value_dur>::const_iterator found = ht.find(key);
    if(found != server->get_hash_table().end()) {
	  // we found it - check duration to see if it expired
      time_t timer;
      timer = time(0);
      if((found->second.d - timer) > 0 || found->second.d < 0){
	    // still valid
	    _return = found->second.v;
      } else{
	    // invalid - delete from table
	    std::map<std::string, value_dur> ht;
        ht = server->get_hash_table();
        ht.erase(key);
        server->set_hash_table(ht);
      }
    } else {
      WatDHTException wdhte;
      WatDHTErrorType error_type;
      wdhte.error_code = error_type.KEY_NOT_FOUND;
      throw wdhte;
    }
  } else {
    boost::shared_ptr<TSocket> socket(new TSocket(next_node.ip, next_node.port));
    boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
    boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
    WatDHTClient client(protocol);
    try {
      transport->open();
      client.get(_return, key);
    } catch (TTransportException e) {
      printf("Exception forwarding get\n");
      printf("Caught exception: %s\n", e.what());
    }
  }//end else
}    
   
void WatDHTHandler::put(const std::string& key,
                        const std::string& val, 
                        const int32_t duration) {
  server->get_wat_state().wait_ge(WatDHTState::SERVER_MIGRATED);

  NodeID next_node;
  WatID key_id;
  key_id.set_using_md5(key);
  server->find_next_hop(next_node, key_id, CLOSEST_CCW, server->get_neighbors(), server->get_rtable());
  
  if(next_node.port == 0){
    WatDHTException wdhte;
    WatDHTErrorType error_type;
    wdhte.error_code = error_type.KEY_NOT_FOUND;
    throw wdhte;
  } else if(server->get_node_id() == next_node){   
    time_t timer;
    timer = time(0);
    time_t dur = timer + duration;
    value_dur value;
    value.v = val;
    std::map<std::string, value_dur> ht;
    if(duration < 0) {
      value.d = -1;
      ht = server->get_hash_table();
      ht[key] = value;
      server->set_hash_table(ht);
    } else if (duration == 0){
      std::map<std::string, value_dur>::iterator it;
      ht = server->get_hash_table();
      it = ht.find(key);
      ht.erase(it);
      server->set_hash_table(ht);
    } else{
      value.d = dur;
      ht = server->get_hash_table();
      ht[key] = value;
      server->set_hash_table(ht);
    }
  } else{
    boost::shared_ptr<TSocket> socket(new TSocket(next_node.ip, next_node.port));
    boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
    boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
    WatDHTClient client(protocol);
    try {
      transport->open();
      client.put(key, val, duration);
      transport->close();
    } catch (TTransportException e) {
      printf("Exception forwarding put\n");
      printf("Caught exception: %s\n", e.what());
    }
  }
}

void WatDHTHandler::join(std::vector<NodeID> & _return, const NodeID& nid) {
  std::vector<NodeID> nodes;  //vector of nodes returned in return path
  NodeID next_node;
  WatID joining_id;
  joining_id.copy_from(nid.id);
  server->find_next_hop(next_node, joining_id, CLOSEST_CCW, server->get_neighbors(), server->get_rtable());
  
  if(nid.port != 0) {
    server->update_neighbors(server->get_neighbors(), nid);
  }

  if(server->get_node_id() == next_node){
    //if we hit the base case
    std::vector<NodeID> n = server->get_neighbors();
    _return.push_back(server->get_node_id());
    _return.insert(_return.end(), n.begin(), n.end());
  } else{
    boost::shared_ptr<TSocket> socket(new TSocket(next_node.ip, next_node.port));
    boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
    boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
    WatDHTClient client(protocol);
    try {
      transport->open();
      client.join(_return, nid);
      _return.push_back(server->get_node_id());
    } catch (TTransportException e) {
      printf("Exception forwarding join\n");
      printf("Caught exception: %s\n", e.what());
    } 
  }
}    

void WatDHTHandler::ping(std::string& _return) {
  server->get_wat_state().wait_ge(WatDHTState::SERVER_JOINED);

  _return = server->get_id().to_string(); 
} 

void WatDHTHandler::maintain(std::vector<NodeID> & _return, 
                             const std::string& id, 
                             const NodeID& nid) {
  server->get_wat_state().wait_ge(WatDHTState::SERVER_JOINED);

  NodeID next_node; 
  WatID joining_id;
  joining_id.copy_from(id);
  server->find_next_hop(next_node, joining_id, CLOSEST, server->get_neighbors(), server->get_rtable());
  
  // Update neighbors and routing table with nid
  if(nid.port != 0) {
    server->update_neighbors(server->get_neighbors(), nid);
  }
  server->update_rtable(nid);

  if(server->get_node_id() == next_node) { // current server is responsible for id
    _return.push_back(server->get_node_id());
  } else {
    // Forward request to next_node
    boost::shared_ptr<TSocket> socket(new TSocket(next_node.ip, next_node.port));
    boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
    boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
    WatDHTClient client(protocol);
    try {
      transport->open();
      client.maintain(_return, id, nid);
      transport->close();
    } catch (TTransportException e) {
      printf("Exception forwarding maintain\n");
      printf("Caught exception: %s\n", e.what());
    }
  }
}

void WatDHTHandler::migrate_kv(std::map<std::string, std::string> & _return, 
                               const std::string& nid) {
  server->get_wat_state().wait_ge(WatDHTState::SERVER_JOINED);

  if(!server->get_is_migrated()) {
    WatDHTException wdhte;
    WatDHTErrorType error_type;
    wdhte.error_code = error_type.OL_MIGRATION_IN_PROGRESS;
    throw wdhte;
  }
	
  if(nid == server->get_neighbors()[2].id) {
    std::map<std::string, value_dur> local_hash = server->get_hash_table();
    std::map<std::string, value_dur>::iterator it = local_hash.begin();
    WatID joining_id;
    joining_id.copy_from(nid);
    while(it != local_hash.end()) {
      WatID hash_key_id;
      hash_key_id.copy_from(it->first);
      if(hash_key_id >= joining_id) {
	    _return[it->first] = it->second.v;
        local_hash.erase(it++);
      } else {
	    ++it;
      }
    }
    server->set_hash_table(local_hash);
  } else {
    WatDHTException wdhte;
    WatDHTErrorType error_type;
    wdhte.error_code = error_type.INCORRECT_MIGRATION_SOURCE;
    wdhte.node = server->get_neighbors()[2];
    throw wdhte;
  }
}

void WatDHTHandler::gossip_neighbors(std::vector<NodeID> & _return, 
                                     const NodeID& nid, 
                                     const std::vector<NodeID> & neighbors) {
  server->get_wat_state().wait_ge(WatDHTState::SERVER_JOINED);

  // Use nid and neighbors to update current server neighbors
  if(nid.port != 0) {
    server->update_neighbors(server->get_neighbors(), nid);
  }
  for(unsigned int i = 0; i < neighbors.size(); i++) {
    if(neighbors[i].port == 0) continue;
    server->update_neighbors(server->get_neighbors(), neighbors[i]);
  }
  
  // Only return neighbors that are not dead
  std::vector<NodeID> n = server->get_neighbors();
  for(unsigned int i = 0; i < n.size(); i++) {
    if(n[i].port == 0) continue;
    boost::shared_ptr<TSocket> socket(new TSocket(n[i].ip, n[i].port));
    boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
    boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
    WatDHTClient client(protocol);
    try {
      transport->open();
      std::string remote_str;
      client.ping(remote_str);
      transport->close();
      _return.push_back(n[i]);
    } catch (TTransportException e) {
      printf("Exception pinging neighbor\n");
      printf("Caught exception: %s\n", e.what());
    }
  }
  _return.push_back(server->get_node_id());

}

void WatDHTHandler::closest_node_cr(NodeID& _return, const std::string& id) {
  server->get_wat_state().wait_ge(WatDHTState::SERVER_JOINED);

  NodeID next_node;
  WatID calling_id;
  calling_id.copy_from(id);
  server->find_next_hop(next_node, calling_id, CLOSEST_CW, server->get_neighbors(), server->get_rtable());
  if(server->get_node_id() == next_node){
    _return = next_node;
  } else{
    boost::shared_ptr<TSocket> socket(new TSocket(next_node.ip, next_node.port));
    boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
    boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
    WatDHTClient client(protocol);
    try {
      transport->open();
      client.closest_node_cr(_return, id);
      transport->close();
    } catch (TTransportException e) {
      printf("Exception forwarding closest_node_cr\n");
      printf("Caught exception: %s\n", e.what());
    } 
  }
}

void WatDHTHandler::closest_node_ccr(NodeID& _return, const std::string& id) {
  server->get_wat_state().wait_ge(WatDHTState::SERVER_JOINED);

  NodeID next_node;
  WatID calling_id;
  calling_id.copy_from(id);
  server->find_next_hop(next_node, calling_id, CLOSEST_CCW, server->get_neighbors(), server->get_rtable());
  if(server->get_node_id() == next_node) {
    _return = next_node;
  } else{
    boost::shared_ptr<TSocket> socket(new TSocket(next_node.ip, next_node.port));
    boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
    boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
    WatDHTClient client(protocol);
    try {
      transport->open();
      client.closest_node_ccr(_return, id);
      transport->close();
    } catch (TTransportException e) {
      printf("Exception forwarding closest_node_ccr\n");
      printf("Caught exception: %s\n", e.what());
    } 
  }
}    
} // namespace WatDHT

