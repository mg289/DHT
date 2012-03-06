#include "WatDHTHandler.h"
#include <string>
#include <vector>
#include <map>

#include "WatDHTServer.h"

namespace WatDHT {

WatDHTHandler::WatDHTHandler(WatDHTServer* dht_server) : server(dht_server) {
  // TO DO
}

WatDHTHandler::~WatDHTHandler() {}

void WatDHTHandler::get(std::string& _return, const std::string& key) {
  // TO DO
  printf("get\n");
}    
    
void WatDHTHandler::put(const std::string& key,
                        const std::string& val, 
                        const int32_t duration) {
  // TO DO
  printf("put\n");
}

void WatDHTHandler::join(std::vector<NodeID> & _return, const NodeID& nid) {
  // TO DO
  printf("join\n");
}    
    

void WatDHTHandler::ping(std::string& _return) {
  // TO DO
  printf("ping\n");
  _return = server->get_id().to_string(); 
} 

void WatDHTHandler::maintain(std::vector<NodeID> & _return, 
                             const std::string& id, 
                             const NodeID& nid) {
  // TO DO
  printf("maintain\n");
}

void WatDHTHandler::migrate_kv(std::map<std::string, std::string> & _return, 
                               const std::string& nid) {
  // TO DO
  printf("migrate_kv\n");
}

void WatDHTHandler::gossip_neighbors(std::vector<NodeID> & _return, 
                                     const NodeID& nid, 
                                     const std::vector<NodeID> & neighbors) {
  // TO DO
  printf("gossip_neighbors\n");
}

void WatDHTHandler::closest_node_cr(NodeID& _return, const std::string& id) {
  // TO DO
  printf("closest_node_cr\n");
}

void WatDHTHandler::closest_node_ccr(NodeID& _return, const std::string& id) {
  // TO DO
  printf("closest_node_ccr\n");
}    
} // namespace WatDHT

