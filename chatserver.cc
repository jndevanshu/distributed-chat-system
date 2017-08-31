#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <sys/time.h>

#include <sys/select.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <errno.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <sstream>
#include <map>
#include <queue>
#include <algorithm>
#include <limits>

using namespace std;

#define MAXLEN 1000

struct addressinfo{
    string IPAddress;
    int port;
};

struct clientinfo{
    string nick;
    int chatRoom;
    sockaddr_in sockAddr;
};

bool V_FLAG;
string ORDERING;
int NUM_SERVERS;
int currentServerIndex;
string THIS_SERVER_IP;
int THIS_SERVER_PORT;

/* Client information mapped to its <IP:port> */
map<string, clientinfo> clientInfoAssignments;

/* FIFO data structures */
struct FIFO_message{
    int sequence;
    string source;
    string message;

    bool operator<(const FIFO_message &msg) const{
        return sequence > msg.sequence;
    }
};
map<int, priority_queue<FIFO_message>> FIFOHoldBackQueue; // a queue for each chatroom
map<int, int> FIFOSequenceMapping; // chatroom -> latest sequence number
map<int, map<string, int>> FIFOSequenceRecieving; // latest sequence observed for a chatroom and a server

/* Causal data structures */
struct CAUSAL_message{
    vector<int> clock;
    int senderIndex;
    string message;
};
map<int, vector<int>> causalClock; //  a clock for each chatroom
map<int, vector<CAUSAL_message>> causalHoldBackQueue; // a queue for each chatroom

/* Total data structures */
struct TOTAL_message{
    int sequence;
    bool isDeliverable; // false for undeliverable ; true otherwise
    string id; // id of the message
    string message;
    int nodeID;
    map<int, int> proposals; // proposal by each server

    bool operator>(const TOTAL_message &msg) const{
        if(sequence > msg.sequence){
            return true;
        }
        else if(sequence == msg.sequence){
            if(nodeID > msg.nodeID){
                return true;
            }
        }
        return false;
    }
    bool operator<(const TOTAL_message &msg) const{
        if(sequence < msg.sequence){
            return true;
        }
        else if(sequence == msg.sequence){
            if(nodeID < msg.nodeID){
                return true;
            }
        }
        return false;
    }
};
map<int, int> P; // highest sequence number proposed per chatroom
map<int, int> A; // highest agreed sequence number per chatroom
map<int, vector<TOTAL_message>> totalHoldBackQueue; // a queue for each chatroom
map<string, int> msgNumbers; // latest message number for each client

map<int, vector<string>> deliveredChats;

bool compareMapValue(pair<int, int> lhs, pair<int, int> rhs)
{
    return lhs.second < rhs.second;
}

/*
 * Returns the IP address from the address.
 * If address is 127.0.0.1:8000, this will return 127.0.0.1
 * If error, returns ""
 */
string getIP(string address){
    int splitIndex = address.find(":");
    if(splitIndex != string::npos){
        return address.substr(0, splitIndex);
    }
    else{
        return "";
    }
}

/*
 * Returns the Port from the address.
 * If address is 127.0.0.1:8000, this will return 8000
 * If error, returns -1
 */
int getPort(string address){
    int splitIndex = address.find(":");
    if(splitIndex != string::npos){
        return stoi(address.substr(splitIndex + 1));
    }
    else{
        return -1;
    }
}

/*
 * Gets the current time
 */
string getCurrentTime(){
    struct timeval tv;
    string time;

    gettimeofday(&tv, NULL);
    time_t currtime = tv.tv_sec;
    char buffer[31];
    strftime(buffer, 30, "%H:%M:%S.", localtime(&currtime));
    time = buffer;
    time += to_string(tv.tv_usec);

    return time;
}

string getCausalClock(vector<int> clock){
    stringstream ss;
    ss << "<";
    for(int i = 0 ; i < clock.size() ; i ++){
        if(i != 0){
            ss << ",";
        }
        ss << clock[i];
    }
    ss << ">";
    return ss.str();
}

/*
 * Sends the message to destination
 */
int sendMessage(int fd, string message, sockaddr_in destination){
    int noBytes;
    if((noBytes = sendto(fd, message.c_str(), message.size(), 0, (struct sockaddr *)&destination, sizeof destination)) == -1){
        fprintf(stderr, "Could not send Message : %s\n", message.c_str());
        perror("Error ");
        return -1;
    }
    return noBytes;
}

/*
 * Recieve the message from source
 */
int recieveMessage(int fd, sockaddr_in &sourceAddress, socklen_t &sourceAddressLen, string &message){
    int noBytes;
    char buffer[MAXLEN + 1];
    if((noBytes = recvfrom(fd, buffer, MAXLEN, 0, (struct sockaddr *)&sourceAddress, &sourceAddressLen)) == -1){
        perror("Could not receive message\nError ");
        return -1;
    }
    buffer[noBytes] = '\0';
    message = buffer;
    return noBytes;
}

/*
 * Checks if the given ordering is correct.
 * Valid orderings: unordered (default), fifo, causal, total
 */

bool checkOrdering(string ordering){
    if(strcasecmp(ordering.c_str(), "unordered") == 0){
        return true;
    }
    else if(strcasecmp(ordering.c_str(), "fifo") == 0){
        return true;
    }
    else if(strcasecmp(ordering.c_str(), "causal") == 0){
        return true;
    }
    else if(strcasecmp(ordering.c_str(), "total") == 0){
        return true;
    }
    return false;
}

/*
 * Fills the sockaddr corresponding to the node IP and port no
 */
struct sockaddr_in getNodeInfo(string IPAddress, int port){
    struct sockaddr_in sockAddr;
    memset(&sockAddr, 0, sizeof sockAddr);
    sockAddr.sin_family = AF_INET;
    sockAddr.sin_port = htons(port);
    inet_pton(AF_INET, IPAddress.c_str(), &(sockAddr.sin_addr));
    return sockAddr;
}

/*
 * Reads the server config file and returns the parsed address structure.
 */
vector<vector<addressinfo>> readFile(char *filename){
    vector<vector<addressinfo>> addressList;
    ifstream istream;
    istream.open(filename);
    string line = "";
    while(getline(istream, line)){
        vector<addressinfo> addresses;
        addressinfo info;
        int separationIndex = line.find(",");
        if(separationIndex == string::npos){
            info.IPAddress = getIP(line);
            info.port = getPort(line);
            addresses.push_back(info);
            addresses.push_back(info);
        }
        else{
            string forwardAddress = line.substr(0,separationIndex);
            info.IPAddress = getIP(forwardAddress);
            info.port = getPort(forwardAddress);
            addresses.push_back(info);

            string bindAddress = line.substr(separationIndex + 1);
            info.IPAddress = getIP(bindAddress);
            info.port = getPort(bindAddress);
            addresses.push_back(info);
        }
        addressList.push_back(addresses);
    }
    istream.close();
    return addressList;
}

/*
 * Converts addressInfo vectors read from the file to nodeInfos
 * At the current server index, pushes NULL
 */
vector<vector<sockaddr_in>> getAllNodes(vector<vector<addressinfo>> addressList, int currentServerIndex){
    vector<vector<sockaddr_in>> nodeList;
    for(int serverIndex = 0 ; serverIndex < addressList.size() ; serverIndex ++){
        if(serverIndex == currentServerIndex){
            vector<sockaddr_in> dummy;
            nodeList.push_back(dummy);
            continue;
        }
        vector<sockaddr_in> nodes;
        nodes.push_back(getNodeInfo(addressList[serverIndex][0].IPAddress, addressList[serverIndex][0].port));
        nodes.push_back(getNodeInfo(addressList[serverIndex][1].IPAddress, addressList[serverIndex][1].port));
        nodeList.push_back(nodes);
    }
    return nodeList;
}

/*
 * Returns the socket to listen at port no.
 * If error, returns -1
 */
int startUDPServerAtPort(int port){
    int serverFD = socket(PF_INET, SOCK_DGRAM, 0);
    if(serverFD < 0){
        perror("Socket Error ");
        return -1;
    }

    struct sockaddr_in servaddr;
    memset(&servaddr, 0, sizeof servaddr);
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = htons(INADDR_ANY);
    servaddr.sin_port = htons(port);

    if(bind(serverFD, (struct sockaddr *)&servaddr, sizeof servaddr) != 0){
        perror("Bind Error ");
        close(serverFD);
        return -1;
    }
    return serverFD;
}

/*
 * Checks if the message is from another server
 * Both are in the network format
 */
bool isMessageFromServer(struct sockaddr_in sourceAddress, vector<vector<sockaddr_in>> nodeList){
    char *sourceIP = inet_ntoa(sourceAddress.sin_addr);
    int sourcePort = ntohs(sourceAddress.sin_port);
    for(int i = 0 ; i < nodeList.size() ; i ++){
        if(nodeList[i].empty()){
            continue;
        }
        char *nodeIP = inet_ntoa(nodeList[i][0].sin_addr);
        int nodePort = ntohs(nodeList[i][0].sin_port);
        if(strcmp(nodeIP, sourceIP) == 0 && nodePort == sourcePort){
            return true;
        }
        nodeIP = inet_ntoa(nodeList[i][1].sin_addr);
        nodePort = ntohs(nodeList[i][1].sin_port);
        if(strcmp(nodeIP, sourceIP) == 0 && nodePort == sourcePort){
            return true;
        }
    }
    return false;
}

/*
 * Sends the message to own clients in the same chatRoom as the parameter passed
 */
int sendSelfClients(int fd, int chatRoom, string message){
    if(deliveredChats.find(chatRoom) == deliveredChats.end()){
        vector<string> messageList;
        deliveredChats[chatRoom] = messageList;
    }
    deliveredChats[chatRoom].push_back(message);
    int noBytes;
    if(V_FLAG){
        fprintf(stdout, "[%s] Sending message '%s' to self clients in chatroom %d\n", getCurrentTime().c_str(), message.c_str(), chatRoom);
    }
    for(map<string, clientinfo>::iterator it = clientInfoAssignments.begin() ; it != clientInfoAssignments.end() ; it ++){
        if((it->second).chatRoom == chatRoom){
            if((noBytes = sendMessage(fd, message, (it->second).sockAddr)) == -1){
                return -1;
            }
        }
    }
    return noBytes;
}

/*
 * Serialises the message for unordered delivery
 */
string serialiseUnorderedMessage(int chatRoom, string text){
    string message = to_string(chatRoom) + " " + text;
    return message;
}

/*
 * Parses the unordered message to retrieve chatroom and the text
 */
int parseUnorderedMessage(int &chatRoom, string &text, string message){
    int spaceIndex = message.find(" ");
    if(spaceIndex == string::npos){
        fprintf(stderr, "Parsing Error!!! Wrong message format: <%s>\n", message.c_str());
        return -1;
    }
    else{
        chatRoom = stoi(message.substr(0, spaceIndex));
        text = message.substr(spaceIndex + 1);
    }
    return 0;
}

/*
 * Handles sending of data to other servers for unordered case
 */
int handleSendingUnordered(int fd, int chatRoom, string message, vector<vector<sockaddr_in>> nodeList){
    string response = serialiseUnorderedMessage(chatRoom, message);
    int noBytes;
    for(int i = 0 ; i < nodeList.size() ; i ++){
        if(nodeList[i].empty()){
            continue;
        }
        if(V_FLAG){
            fprintf(stdout, "[%s] Sending Unordered message '%s' to server %d\n", getCurrentTime().c_str(), response.c_str(), i);
        }
        if((noBytes = sendMessage(fd, response, nodeList[i][0])) == -1){
            return -1;
        }   
    }
}

/*
 * Handles recieving of an unordered message
 */
int handleRecievingUnordered(int fd, string message){
    int chatRoom;
    string text;
    if(parseUnorderedMessage(chatRoom, text, message) != 0){
        return -1;
    }
    if(V_FLAG){
        fprintf(stdout, "[%s] Trying to send (Unordered) message '%s' to clients in chatroom %d\n", getCurrentTime().c_str(), text.c_str(), chatRoom);
    }
    sendSelfClients(fd, chatRoom, text);
}

/*
 * Serialises the message for FIFO delivery
 */
string serialiseFIFOMessage(int sequenceNumber, int chatRoom, string text){
    string message = to_string(sequenceNumber) + " " + to_string(chatRoom) + " " + text;
    return message;
}

/*
 * Parses FIFO message to retrieve seq #, chatroom and text
 */
int parseFIFOMessage(int &sequenceNumber, int &chatRoom, string &text, string message){
    int spaceIndex = message.find(" ");
    if(spaceIndex == string::npos){
        fprintf(stderr, "Wrong message format: <%s>\n", message.c_str());
        return -1;
    }
    else{
        sequenceNumber = stoi(message.substr(0, spaceIndex));
        text = message.substr(spaceIndex + 1);
        spaceIndex = text.find(" ");
        if(spaceIndex == string::npos){
            fprintf(stderr, "Wrong message format: <%s>\n", message.c_str());
            return -1;
        }
        else{
            chatRoom = stoi(text.substr(0, spaceIndex));
            text = text.substr(spaceIndex + 1);
        }
    }
    return 0;
}

/*
 * Handles sending of data to other servers for FIFO case
 */
int handleSendingFIFO(int fd, int chatRoom, string message, vector<vector<sockaddr_in>> nodeList){
    if(FIFOSequenceMapping.find(chatRoom) == FIFOSequenceMapping.end()){
        FIFOSequenceMapping[chatRoom] = 0;
    }
    FIFOSequenceMapping[chatRoom] ++;
    string response = serialiseFIFOMessage(FIFOSequenceMapping[chatRoom], chatRoom, message);
    int noBytes;
    for(int i = 0 ; i < nodeList.size() ; i ++){
        if(nodeList[i].empty()){
            continue;
        }
        if(V_FLAG){
            fprintf(stdout, "[%s] Sending FIFO message '%s' to server %d\n", getCurrentTime().c_str(), response.c_str(), i);
        }
        if((noBytes = sendMessage(fd, response, nodeList[i][0])) == -1){
            return -1;
        }   
    }
}

bool isFIFOdeliverable(int sequenceNumber, int chatRoom, string source){
    if(V_FLAG){
        fprintf(stdout, "[%s] Expecting message with sequence: %d\n", getCurrentTime().c_str(), FIFOSequenceRecieving[chatRoom][source] + 1);
        fprintf(stdout, "[%s] You have provided a message with sequence: %d\n", getCurrentTime().c_str(), sequenceNumber);
    }
    if(FIFOSequenceRecieving[chatRoom][source] + 1 == sequenceNumber){
        if(V_FLAG){
            fprintf(stdout, "[%s] FIFO message is deliverable\n", getCurrentTime().c_str());
        }
        return true;
    }
    if(V_FLAG){
        fprintf(stdout, "[%s] FIFO message is not deliverable\n", getCurrentTime().c_str());
    }
    return false;
}

int FIFODeliver(int fd, int chatRoom){
    priority_queue<FIFO_message> fifoQueue = FIFOHoldBackQueue[chatRoom];
    if(fifoQueue.empty()){
        return 0;
    }
    FIFO_message msg = fifoQueue.top();
    while(isFIFOdeliverable(msg.sequence, chatRoom, msg.source)){
        fifoQueue.pop();
        sendSelfClients(fd, chatRoom, msg.message);
        FIFOSequenceRecieving[chatRoom][msg.source] ++;
        if(fifoQueue.empty()){
            break;
        }
        msg = fifoQueue.top();
    }
    FIFOHoldBackQueue[chatRoom] = fifoQueue;
    return 0;
}

/*
 * Handles recieving of a fifo message
 */
int handleRecievingFIFO(int fd, string message, string sourceKey){
    int sequenceNumber, chatRoom;
    string text;
    if(parseFIFOMessage(sequenceNumber, chatRoom, text, message) != 0){
        return -1;
    }
    if(V_FLAG){
        fprintf(stdout, "[%s] Trying to send (FIFO) message '%s' to clients in chatroom %d\n", getCurrentTime().c_str(), text.c_str(), chatRoom);
    }
    if(FIFOSequenceRecieving.find(chatRoom) == FIFOSequenceRecieving.end()){
        map<string, int> serverEntry;
        FIFOSequenceRecieving[chatRoom] = serverEntry;
    }
    if(FIFOSequenceRecieving[chatRoom].find(sourceKey) == FIFOSequenceRecieving[chatRoom].end()){
        FIFOSequenceRecieving[chatRoom][sourceKey] = 0;
    }
    if(FIFOHoldBackQueue.find(chatRoom) == FIFOHoldBackQueue.end()){
        priority_queue<FIFO_message> fifoQueue;
        FIFOHoldBackQueue[chatRoom] = fifoQueue;
    }

    FIFO_message msg;
    msg.sequence = sequenceNumber;
    msg.message = text;
    msg.source = sourceKey;
    FIFOHoldBackQueue[chatRoom].push(msg);

    FIFODeliver(fd, chatRoom);
    return 0;
}

/*
 * Serialises the message for Causal delivery
 */
string serialiseCausalMessage(vector<int> clock, int myIndex, int chatRoom, string text){
    stringstream message;
    message << "<";
    for(int i = 0 ; i < clock.size() - 1 ; i ++){
        message << clock[i] << ",";
    }
    message << clock[clock.size() - 1] << ">";
    message << myIndex << " ";
    message << chatRoom << " ";
    message << text;
    return message.str();
}

/*
 * Parses Causal message to retrieve clock, chatroom and text
 */
int parseCausalMessage(vector<int> &clock, int &sender, int &chatRoom, string &text, string message){
    int leftAngularIndex = message.find("<");
    int rightAngularIndex = message.find(">");
    if(leftAngularIndex == string::npos || rightAngularIndex == string::npos){
        fprintf(stderr, "Wrong message format: <%s>\n", message.c_str());
        return -1;
    }
    else{
        string clock_str = message.substr(leftAngularIndex + 1, rightAngularIndex - leftAngularIndex - 1);
        int commaIndex = clock_str.find(",");
        if(commaIndex == string::npos){
            fprintf(stderr, "Wrong message format: <%s>\n", message.c_str());
            return -1;
        }
        while(commaIndex != string::npos){
            clock.push_back(stoi(clock_str.substr(0,commaIndex)));
            clock_str = clock_str.substr(commaIndex + 1);
            commaIndex = clock_str.find(",");
        }
        clock.push_back(stoi(clock_str));
        text = message.substr(rightAngularIndex + 1);
        int spaceIndex = text.find(" ");
        if(spaceIndex == string::npos){
            fprintf(stderr, "Wrong message format: <%s>\n", message.c_str());
            return -1;
        }
        sender = stoi(text.substr(0, spaceIndex));
        text = text.substr(spaceIndex + 1);
        spaceIndex = text.find(" ");
        if(spaceIndex == string::npos){
            fprintf(stderr, "Wrong message format: <%s>\n", message.c_str());
            return -1;
        }
        chatRoom = stoi(text.substr(0, spaceIndex));
        text = text.substr(spaceIndex + 1);
    }
    return 0;
}

/*
 * sends message for causal ordering
 */
int handleSendingCausal(int fd, int chatRoom, string message, vector<vector<sockaddr_in>> nodeList){
    if(causalClock.find(chatRoom) == causalClock.end()){
        vector<int> clock;
        for(int i = 0 ; i < NUM_SERVERS ; i ++){
            clock.push_back(0);
        }
        causalClock[chatRoom] = clock;
    }
    causalClock[chatRoom][currentServerIndex] ++;
    string response = serialiseCausalMessage(causalClock[chatRoom], currentServerIndex, chatRoom, message);
    int noBytes;
    for(int i = 0 ; i < nodeList.size() ; i ++){
        if(nodeList[i].empty()){
            continue;
        }
        if(V_FLAG){
            fprintf(stdout, "[%s] Sending Causal message '%s' to server %d\n", getCurrentTime().c_str(), response.c_str(), i);
        }
        if((noBytes = sendMessage(fd, response, nodeList[i][0])) == -1){
            return -1;
        }   
    }
}

/*
 * Checks if the causal message is deliverable
 */
bool isCausalDeliverable(vector<int> msgClock, int senderIndex, int chatRoom){
    if(V_FLAG){
        fprintf(stdout, "[%s] Checking validity\n", getCurrentTime().c_str());
        fprintf(stdout, "[%s] Message clock: %s, Sender Index: %d\n", getCurrentTime().c_str(), getCausalClock(msgClock).c_str(), senderIndex);
        fprintf(stdout, "[%s] My clock: %s\n", getCurrentTime().c_str(), getCausalClock(causalClock[chatRoom]).c_str());
    }
    for(int i = 0 ; i < msgClock.size() ; i ++){
        if(i == senderIndex){
            continue;
        }
        if(msgClock[i] > causalClock[chatRoom][i]){
            return false;
        }
    }
    if(msgClock[senderIndex] != causalClock[chatRoom][senderIndex] + 1){
        return false;
    }
    return true;
}

/*
 * Delivers the causal message and clears the queue accordingly
 */
int causalDeliver(int fd, int chatRoom, string text){
    bool progress;
    vector<CAUSAL_message> causalQueue = causalHoldBackQueue[chatRoom];
    while(true){
        progress = false;
        for(vector<CAUSAL_message>::iterator it = causalQueue.begin() ; it != causalQueue.end() ; ){
            if(isCausalDeliverable(it->clock, it->senderIndex, chatRoom)){
                if(V_FLAG){
                    fprintf(stdout, "[%s] Delivering message '%s' to client\n", getCurrentTime().c_str(), (it->message).c_str());
                }
                sendSelfClients(fd, chatRoom, it->message);
                causalClock[chatRoom][it->senderIndex] ++;
                it = causalQueue.erase(it);
                progress = true;
            }
            else{
                it ++;
            }
        }
        if(!progress){
            break;
        }
    }    
    causalHoldBackQueue[chatRoom] = causalQueue;
    return 0;
}

/*
 * Handles recieving of causal message
 */
int handleRecievingCausal(int fd, string message){
    vector<int> msgClock;
    int senderIndex;
    int chatRoom;
    string text;
    if(parseCausalMessage(msgClock, senderIndex, chatRoom, text, message) != 0){
        return -1;
    }
    if(causalHoldBackQueue.find(chatRoom) == causalHoldBackQueue.end()){
        vector<CAUSAL_message> causalQueue;
        causalHoldBackQueue[chatRoom] = causalQueue;
    }
    if(causalClock.find(chatRoom) == causalClock.end()){
        vector<int> clock;
        for(int i = 0 ; i < NUM_SERVERS ; i ++){
            clock.push_back(0);
        }
        causalClock[chatRoom] = clock;
    }
    if(V_FLAG){
        fprintf(stdout, "[%s] My clock: %s\n", getCurrentTime().c_str(), getCausalClock(causalClock[chatRoom]).c_str());
        fprintf(stdout, "[%s] Trying to send (Causal) message '%s' to clients in chatroom %d\n", getCurrentTime().c_str(), text.c_str(), chatRoom);
    }
    CAUSAL_message msg;
    msg.clock = msgClock;
    msg.senderIndex = senderIndex;
    msg.message = text;
    causalHoldBackQueue[chatRoom].push_back(msg);
    
    if(isCausalDeliverable(msgClock, senderIndex, chatRoom)){
        causalDeliver(fd, chatRoom, text);
    }
    return 0;
}

/*
 * Creates id for text
 */
string createID(string clientKey, string serverKey, int msgNum){
    stringstream ss;
    ss << clientKey << ":" << serverKey << ":" << msgNum;
    return ss.str();
}

/*
 * Serialises the message for Total delivery
 */
string serialiseTotalMessage(string type, int sequenceNumber, int serverIndex, int chatRoom, string id, string text){
    stringstream ss;
    if(strcasecmp(type.c_str(), "init") == 0){
        ss << 0 << " " << serverIndex << " " << chatRoom << " " << id << " " << text;
    }
    else if(strcasecmp(type.c_str(), "confirm") == 0){
        ss << 1 << " " << sequenceNumber << " " << serverIndex << " " << chatRoom << " " << id << " " << text;
    }
    else if(strcasecmp(type.c_str(), "proposal") == 0){
        ss << 2 << " " << sequenceNumber << " " << serverIndex << " " << chatRoom << " " << id << " " << text;
    }
    else{
        fprintf(stderr, "Something went wrong, wrong type (%s) while serialising total order message\n", type.c_str());
        return "";
    }

    return ss.str();
}

/*
 * Parses total ordered message to retrieve necessary info
 */
int parseTotalMessage(int &type, int &sequenceNumber, int &serverIndex, int &chatRoom, string &id, string &text, string message){
    int spaceIndex = message.find(" ");
    if(spaceIndex == string::npos){
        fprintf(stderr, "Wrong message format: <%s>\n", message.c_str());
        return -1;
    }
    type = stoi(message.substr(0, spaceIndex));
    if(type == 0){
        // init
        sequenceNumber = -1;
        text = message.substr(spaceIndex + 1);
        spaceIndex = text.find(" ");
        if(spaceIndex == string::npos){
            fprintf(stderr, "Wrong message format: <%s>\n", message.c_str());
            return -1;
        }
        serverIndex = stoi(text.substr(0, spaceIndex));
        text = text.substr(spaceIndex + 1);
        spaceIndex = text.find(" ");
        if(spaceIndex == string::npos){
            fprintf(stderr, "Wrong message format: <%s>\n", message.c_str());
            return -1;
        }
        chatRoom = stoi(text.substr(0, spaceIndex));
        text = text.substr(spaceIndex + 1);
        spaceIndex = text.find(" ");
        if(spaceIndex == string::npos){
            fprintf(stderr, "Wrong message format: <%s>\n", message.c_str());
            return -1;
        }
        id = text.substr(0, spaceIndex);
        text = text.substr(spaceIndex + 1);
    }
    else if(type == 1 || type == 2){
        // 1->confirm
        // 2->proposal
        text = message.substr(spaceIndex + 1);
        spaceIndex = text.find(" ");
        if(spaceIndex == string::npos){
            fprintf(stderr, "Wrong message format: <%s>\n", message.c_str());
            return -1;
        }
        sequenceNumber = stoi(text.substr(0, spaceIndex));
        text = text.substr(spaceIndex + 1);
        spaceIndex = text.find(" ");
        if(spaceIndex == string::npos){
            fprintf(stderr, "Wrong message format: <%s>\n", message.c_str());
            return -1;
        }
        serverIndex = stoi(text.substr(0, spaceIndex));
        text = text.substr(spaceIndex + 1);
        spaceIndex = text.find(" ");
        if(spaceIndex == string::npos){
            fprintf(stderr, "Wrong message format: <%s>\n", message.c_str());
            return -1;
        }
        chatRoom = stoi(text.substr(0, spaceIndex));
        text = text.substr(spaceIndex + 1);
        spaceIndex = text.find(" ");
        if(spaceIndex == string::npos){
            fprintf(stderr, "Wrong message format: <%s>\n", message.c_str());
            return -1;
        }
        id = text.substr(0, spaceIndex);
        text = text.substr(spaceIndex + 1);
    }
    else{
        fprintf(stderr, "Something went wrong, wrong type (%d) while parsing total order message: [%s]\n", type, message.c_str());
        return -1;
    }
    return 0;
}

/*
 * sends message for total ordering
 */
int handleSendingTotal(int fd, int chatRoom, string messageID, string message, vector<vector<sockaddr_in>> nodeList){
    string response = serialiseTotalMessage("init", -1, currentServerIndex, chatRoom, messageID, message);
    int noBytes;
    for(int i = 0 ; i < nodeList.size() ; i ++){
        if(nodeList[i].empty()){
            continue;
        }
        if(V_FLAG){
            fprintf(stdout, "[%s] Sending Total message '%s' to server %d\n", getCurrentTime().c_str(), response.c_str(), i);
        }
        if((noBytes = sendMessage(fd, response, nodeList[i][0])) == -1){
            return -1;
        }   
    }
}

/*
 * Handles recieving of total message
 */
int handleRecievingTotal(int fd, string message, sockaddr_in sourceAddress, vector<vector<sockaddr_in>> nodeList){
    int noBytes;
    int type;
    int sequenceNumber;
    int serverIndex;
    int chatRoom;
    string textID;
    string text;
    if(parseTotalMessage(type, sequenceNumber, serverIndex, chatRoom, textID, text, message) != 0){
        return -1;
    }
    if(P.find(chatRoom) == P.end()){
        P[chatRoom] = 0;
    }
    if(A.find(chatRoom) == A.end()){
        A[chatRoom] = 0;
    }
    if(totalHoldBackQueue.find(chatRoom) == totalHoldBackQueue.end()){
        vector<TOTAL_message> pqueue;
        totalHoldBackQueue[chatRoom] = pqueue;
    }
    if(type == 0){
        // init
        if(V_FLAG){
            fprintf(stdout, "[%s] Init message recieved '%s'\n", getCurrentTime().c_str(), message.c_str());
        }
        P[chatRoom] = max(P[chatRoom], A[chatRoom]) + 1;
        TOTAL_message msg;
        msg.sequence = P[chatRoom];
        msg.nodeID = -1;
        msg.isDeliverable = false;
        msg.id = textID;
        msg.message = text;
        vector<TOTAL_message> pqueue = totalHoldBackQueue[chatRoom];
        pqueue.push_back(msg);
        push_heap(pqueue.begin(), pqueue.end(), greater<TOTAL_message>());
        totalHoldBackQueue[chatRoom] = pqueue;
        string response = serialiseTotalMessage("proposal", msg.sequence, currentServerIndex, chatRoom, msg.id, msg.message);
        if(V_FLAG){
            fprintf(stdout, "[%s] Sending a proposal '%s'\n", getCurrentTime().c_str(), response.c_str());
        }
        if((noBytes = sendMessage(fd, response, sourceAddress)) == -1){
            return -1;
        }
    }
    else if(type == 1){
        // confirm
        if(V_FLAG){
            fprintf(stdout, "[%s] Confirm message recieved '%s'\n", getCurrentTime().c_str(), message.c_str());
        }
        vector<TOTAL_message> pqueue = totalHoldBackQueue[chatRoom];
        for(vector<TOTAL_message>::iterator it = pqueue.begin() ; it != pqueue.end() ; it ++){
            if(strcasecmp((it->id).c_str(), textID.c_str()) == 0){
                it->sequence = sequenceNumber;
                it->isDeliverable = true;
                it->nodeID = serverIndex;
                A[chatRoom] = max(A[chatRoom], sequenceNumber);
                break;
            }
        }
        make_heap(pqueue.begin(), pqueue.end(), greater<TOTAL_message> ());
        while(pqueue.front().isDeliverable){
            pop_heap(pqueue.begin(), pqueue.end(), greater<TOTAL_message>());
            TOTAL_message msg = pqueue.back();
            if(V_FLAG){
                fprintf(stdout, "[%s] Deliering the message: '%s'\n", getCurrentTime().c_str(), msg.message.c_str());
            }
            pqueue.pop_back();
            sendSelfClients(fd, chatRoom, msg.message);
            if(pqueue.empty()){
                break;
            }
        }
        totalHoldBackQueue[chatRoom] = pqueue;
    }
    else if(type == 2){
        // proposal
        if(V_FLAG){
            fprintf(stdout, "[%s] Proposal message recieved '%s'\n", getCurrentTime().c_str(), message.c_str());
        }
        int maxProposal = -1;
        int maxProposalNode = -1;
        vector<TOTAL_message> pqueue = totalHoldBackQueue[chatRoom];
        for(vector<TOTAL_message>::iterator it = pqueue.begin() ; it != pqueue.end() ; it ++){
            if(strcasecmp((it->id).c_str(), textID.c_str()) == 0){
                (it->proposals)[serverIndex] = sequenceNumber;
                if(V_FLAG){
                    fprintf(stdout, "[%s] Number of proposals now: %d\n", getCurrentTime().c_str(), (it->proposals).size());
                }
                if((it->proposals).size() == NUM_SERVERS){
                    // all proposals recieved
                    if(V_FLAG){
                        fprintf(stdout, "[%s] All proposals are recieved\n", getCurrentTime().c_str());
                    }
                    pair<int, int> maxProposalPair = (*max_element((it->proposals).begin(), (it->proposals).end(), compareMapValue));
                    maxProposal = maxProposalPair.second;
                    maxProposalNode = maxProposalPair.first;
                    it->sequence = maxProposal;
                    it->nodeID = maxProposalNode;
                    it->isDeliverable = true;
                    A[chatRoom] = max(A[chatRoom], maxProposal);
                }
                break;
            }
        }
        totalHoldBackQueue[chatRoom] = pqueue;
        if(maxProposal != -1){
            make_heap(pqueue.begin(), pqueue.end(), greater<TOTAL_message>());
            while(pqueue.front().isDeliverable){
                pop_heap(pqueue.begin(), pqueue.end(), greater<TOTAL_message>());
                TOTAL_message msg = pqueue.back();
                pqueue.pop_back();
                sendSelfClients(fd, chatRoom, msg.message);
                if(pqueue.empty()){
                    break;
                }
            }
            totalHoldBackQueue[chatRoom] = pqueue;
            string response = serialiseTotalMessage("confirm", maxProposal, maxProposalNode, chatRoom, textID, text);
            for(int i = 0 ; i < nodeList.size() ; i ++){
                if(nodeList[i].empty()){
                    continue;
                }
                if(V_FLAG){
                    fprintf(stdout, "[%s] Sending Total message confirm '%s' to server %d\n", getCurrentTime().c_str(), response.c_str(), i);
                }
                if((noBytes = sendMessage(fd, response, nodeList[i][0])) == -1){
                    return -1;
                }   
            }
        }
        else{
            if(V_FLAG){
                fprintf(stdout, "[%s] In Proposal: Message '%s' just put in queue\n", getCurrentTime().c_str(), message.c_str());
            }
        }
    }
}

/*
 * Handles Client Command
 * Valid Commands:
 * 1. /join <roomID> : Join a particular chat room
 * 2. /part : leave the current chat room
 * 3. /nick : set user's nickname ; default is IP:port
 * 4. /quit : server deletes this client from the list of active clients
 * If the first letter is not '/', then the command is infact a message that needs to be delivered to every other server
 */
int handleClientCommand(string command, int fd, struct sockaddr_in sourceAddress, vector<vector<sockaddr_in>> nodeList){
    if(command.size() == 0){
        // invalid command
        return -1;
    }
    int noBytes;
    string key = inet_ntoa(sourceAddress.sin_addr);
    key += ":";
    key += to_string(ntohs(sourceAddress.sin_port));
    
    if(command.at(0) == '/'){
        // Command
        if(V_FLAG){
            fprintf(stdout, "[%s] <%s>: %s\n", getCurrentTime().c_str(), key.c_str(), command.c_str());
        }
        bool isCommandCorrect;
        string response;
        int spaceIndex = command.find(" ");
        if(spaceIndex != string::npos){
            if(strcasecmp(command.substr(1, spaceIndex-1).c_str(), "join") == 0){
                int chatRoom = stoi(command.substr(spaceIndex + 1));
                if(strcasecmp(ORDERING.c_str(), "total") == 0){
                    msgNumbers[key] = 0;
                }
                clientInfoAssignments[key].chatRoom = chatRoom;
                clientInfoAssignments[key].sockAddr = getNodeInfo(inet_ntoa(sourceAddress.sin_addr), ntohs(sourceAddress.sin_port));
                if(clientInfoAssignments[key].nick.size() == 0){
                    clientInfoAssignments[key].nick = key;
                }
                response = "+OK, You have been added to chatroom " + to_string(chatRoom);
                isCommandCorrect = true;
            }
            else if(strcasecmp(command.substr(1, spaceIndex-1).c_str(), "nick") == 0){
                string nickName = command.substr(spaceIndex + 1);
                clientInfoAssignments[key].nick = nickName;
                response = "+OK, Nick set to " + nickName;
                isCommandCorrect = true;
            }
            else{
                isCommandCorrect = false;
            }
        }
        else if(strcasecmp(command.substr(1).c_str(), "part") == 0){
            if(clientInfoAssignments.find(key) == clientInfoAssignments.end()){
                response = "-ERR You are not part of any chatroom yet!";
            }
            else{
                response = "+OK, You have left chatroom " + to_string(clientInfoAssignments[key].chatRoom);
                clientInfoAssignments.erase(key);
                msgNumbers.erase(key);
            }
            isCommandCorrect = true;
        }
        else if(strcasecmp(command.substr(1).c_str(), "quit") == 0){
            if(clientInfoAssignments.find(key) != clientInfoAssignments.end()){
                clientInfoAssignments.erase(key);
                msgNumbers.erase(key);
            }
            isCommandCorrect = true;
        }
        else{
            // wrong command
            isCommandCorrect = false;
        }

        if(!isCommandCorrect){
            response = "-ERR ";
            response += command;
            response += " Command Unrecognised";
        }
        if((noBytes = sendMessage(fd, response, sourceAddress)) == -1){
            return -1;
        }
    }
    else{
        // a message
        if(clientInfoAssignments.find(key) == clientInfoAssignments.end()){
            // user has not been assigned a chatroom
            // cannot send a message
            // respond with -ERR
            string response = "-ERR You have not been assigned a chatroom ; Cannot deliver message";
            if(V_FLAG){
                fprintf(stdout, "[%s] User <%s> tried to send message %s before joining any chatroom\n", getCurrentTime().c_str(), key.c_str(), command.c_str());
            }
            if((noBytes = sendMessage(fd, response, sourceAddress)) == -1){
                return -1;
            }
            return 0;
        }
        string response = "<" + clientInfoAssignments[key].nick + "> " + command;

        if(strcasecmp(ORDERING.c_str(), "unordered") == 0){
            sendSelfClients(fd, clientInfoAssignments[key].chatRoom, response);
            handleSendingUnordered(fd, clientInfoAssignments[key].chatRoom, response, nodeList);
        }
        else if(strcasecmp(ORDERING.c_str(), "fifo") == 0){
            sendSelfClients(fd, clientInfoAssignments[key].chatRoom, response);
            handleSendingFIFO(fd, clientInfoAssignments[key].chatRoom, response, nodeList);
        }
        else if(strcasecmp(ORDERING.c_str(), "causal") == 0){
            sendSelfClients(fd, clientInfoAssignments[key].chatRoom, response);
            handleSendingCausal(fd, clientInfoAssignments[key].chatRoom, response, nodeList);
        }
        else if(strcasecmp(ORDERING.c_str(), "total") == 0){
            string messageID = createID(key, THIS_SERVER_IP + ":" + to_string(THIS_SERVER_PORT), msgNumbers[key]);
            P[clientInfoAssignments[key].chatRoom] = max(P[clientInfoAssignments[key].chatRoom], A[clientInfoAssignments[key].chatRoom]) + 1;
            TOTAL_message msg;
            msg.sequence = P[clientInfoAssignments[key].chatRoom];
            msg.isDeliverable = false;
            msg.id = messageID;
            msg.nodeID = -1;
            msg.message = response;
            msg.proposals[currentServerIndex] = P[clientInfoAssignments[key].chatRoom];
            vector<TOTAL_message> pqueue = totalHoldBackQueue[clientInfoAssignments[key].chatRoom];
            pqueue.push_back(msg);
            push_heap(pqueue.begin(), pqueue.end(), greater<TOTAL_message>());
            totalHoldBackQueue[clientInfoAssignments[key].chatRoom] = pqueue;
            msgNumbers[key] ++;
            handleSendingTotal(fd, clientInfoAssignments[key].chatRoom, messageID, response, nodeList);
        }
        else{
            fprintf(stderr, "Trying to send client message to the servers....Wrong ordering specified!\nAborting%s\n", ORDERING.c_str());
            return -1;
        }
    }
}

/*
 * Handles Server Command
 */
int handleServerCommand(string command, int fd, sockaddr_in sourceAddress, vector<vector<sockaddr_in>> nodeList){
    if(command.size() == 0){
        // invalid command
        return -1;
    }
    if(strcasecmp(ORDERING.c_str(), "unordered") == 0){
        handleRecievingUnordered(fd, command);
    }
    else if(strcasecmp(ORDERING.c_str(), "fifo") == 0){
        stringstream ss;
        ss << inet_ntoa(sourceAddress.sin_addr) << ":" << ntohs(sourceAddress.sin_port);
        handleRecievingFIFO(fd, command, ss.str());
    }
    else if(strcasecmp(ORDERING.c_str(), "causal") == 0){
        handleRecievingCausal(fd, command);
    }
    else if(strcasecmp(ORDERING.c_str(), "total") == 0){
        handleRecievingTotal(fd, command, sourceAddress, nodeList);
    }
    else{
        fprintf(stderr, "Trying to send client message to the servers....Wrong ordering specified!\nAborting%s\n", ORDERING.c_str());
        return -1;
    }
}

/*
 * Server Behaviour
 */
int doServerBehaviour(int fd, vector<vector<sockaddr_in>> nodeList){
    string message;
    int noBytes;
    struct sockaddr_in sourceAddress;
    socklen_t sourceAddressLen = sizeof sourceAddress;
    if((noBytes = recieveMessage(fd, sourceAddress, sourceAddressLen, message)) <= 0){
        return -1;
    }
    if(isMessageFromServer(sourceAddress, nodeList)){
        if(V_FLAG){
            fprintf(stdout, "[%s] <%s:%d> (Server): %s\n", getCurrentTime().c_str(), inet_ntoa(sourceAddress.sin_addr), ntohs(sourceAddress.sin_port), message.c_str());
        }
        handleServerCommand(message, fd, sourceAddress, nodeList);
    }
    else{
        if(V_FLAG){
            fprintf(stdout, "[%s] <%s:%d> (Client): %s\n", getCurrentTime().c_str(), inet_ntoa(sourceAddress.sin_addr), ntohs(sourceAddress.sin_port), message.c_str());
        }
        handleClientCommand(message, fd, sourceAddress, nodeList);
    }

    for(map<int, vector<string>>::iterator it = deliveredChats.begin() ; it != deliveredChats.end() ; it++){
        fprintf(stdout, "For chatroom: %d, Chats delivered: \n", it->first);
        for(int i = 0 ; i < (it->second).size() ; i++){
            fprintf(stdout, "Message: %s\n", (it->second)[i].c_str());
        }
    }
}

/*
 * Starts the server and calls server behaviour eternally ; index starts from 0
 */
int startServer(vector<vector<addressinfo>> serverList, int currentServerIndex){
    int serverFD = startUDPServerAtPort(serverList[currentServerIndex][1].port);
    if(V_FLAG){
        string time = getCurrentTime();
        fprintf(stdout, "[%s], S%d: Server started at port: %d\n", time.c_str(), currentServerIndex, serverList[currentServerIndex][1].port);
    }
    vector<vector<sockaddr_in>> nodeList = getAllNodes(serverList, currentServerIndex);
    fd_set readFD;
    int maxFD;
    int retValue;

    while(true){
        FD_ZERO(&readFD);
        maxFD = -1;

        FD_SET(serverFD, &readFD);
        maxFD = max(maxFD, serverFD);
        maxFD += 1;
        if(V_FLAG){
            fprintf(stdout, "Listening!!!\n");
        }
        retValue = select(maxFD, &readFD, NULL, NULL, NULL);

        if(retValue == -1){
            perror("Select Error: ");
        }
        else if(retValue == 0){
            fprintf(stderr, "No FD is ready.... Still returned Something Wrong!!! Trying again...\n");
        }
        else{
            if(FD_ISSET(serverFD, &readFD)){
                doServerBehaviour(serverFD, nodeList);
            }
        }
    }
}

int main(int argc, char *argv[])
{
    if (argc < 2) {
        fprintf(stderr, "*** Author: Devanshu Jain (devjain)\n");
        exit(1);
    }

    ORDERING = "unordered";
    int c;
    while((c = getopt(argc, argv, "vo:")) != -1){
        switch(c){
            case 'v':
                V_FLAG = true;
                break;
            case 'o':
                ORDERING = optarg;
                if(!checkOrdering(ORDERING.c_str())){
                    fprintf(stderr, "[%s]: Wrong Ordering option\n", ORDERING);
                    exit(2);
                }
                break;
            case '?':
                fprintf(stderr, "Unknown option `-%c`.\n", optopt);
                exit(1);
            default:
                abort();
        }
    }

    string filename = argv[optind];
    currentServerIndex = atoi(argv[optind + 1]);
    if(currentServerIndex <= 0){
        fprintf(stderr, "[%d]: Index cannot be non-positive\n", currentServerIndex);
        exit(2);
    }
    vector<vector<addressinfo>> servAddressList = readFile(const_cast<char *>(filename.c_str()));
    if(currentServerIndex > servAddressList.size()){
        fprintf(stderr, "[%d]: Index cannot be greater than the address list length (%d)\n", currentServerIndex, servAddressList.size());
        exit(2);
    }
    NUM_SERVERS = servAddressList.size();
    THIS_SERVER_IP = servAddressList[currentServerIndex - 1][1].IPAddress;
    THIS_SERVER_PORT = servAddressList[currentServerIndex - 1][1].port;
    
    currentServerIndex = currentServerIndex - 1;
    
    startServer(servAddressList, currentServerIndex);
    return 0;
}