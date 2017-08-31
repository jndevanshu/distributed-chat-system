#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>

#include <sys/select.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <errno.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <string>
#include <algorithm>
#include <cstring>

using namespace std;

#define MAXLEN 1000

struct nodeinfo{
    sockaddr_in sockAddr;
    int fd;
};

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
 * Populates the socket and sockaddr corresponding to the server IP and port
 * Return 0 on success; -1 on error
 */
int getNodeInfo(string IPAddress, int port, nodeinfo &nodeInfo){
    int serverFD = socket(PF_INET, SOCK_DGRAM, 0);
    if(serverFD < 0){
        perror("Socket Error ");
        nodeinfo dummy;
        return 1;
    }
    struct sockaddr_in destination;
    memset(&destination, 0, sizeof destination);
    destination.sin_family = AF_INET;
    destination.sin_port = htons(port);
    inet_pton(AF_INET, IPAddress.c_str(), &(destination.sin_addr));
    nodeInfo.sockAddr = destination;
    nodeInfo.fd = serverFD;
    return 0;
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
 * Listens to the fd; read data and send to server ; wait for response and print response to terminal
 * Q - Does client waits for response from the server ?
 */
int doUserBehaviour(int userFD, int serverFD, struct sockaddr_in destination){
    char buffer[MAXLEN + 1];
    int noBytes;
    fprintf(stderr, "Receiving message from user!!!\n");

    if((noBytes = read(userFD, buffer, MAXLEN)) == -1){
        perror("Read Failure ");
        return -1;
    }
    if(buffer[noBytes - 2] == '\r'){
        buffer[noBytes - 2] = '\0';
    }
    else if(buffer[noBytes - 1] == '\n'){
        buffer[noBytes - 1] = '\0';
    }
    else{
        fprintf(stderr, "Something wrong while reading user's message/command (CRLF)\nMessage: %s\nLength: %d\n", buffer, strlen(buffer));
        return -1;
    }
    if((noBytes = sendMessage(serverFD, buffer, destination)) == -1){
        return -1;
    }
    if(strcasecmp(buffer, "quit") == 0){
        return 1;
    }
    struct sockaddr_in theirAddr;
    socklen_t theirAddrLen = sizeof theirAddr;
    string message;
    if((noBytes = recieveMessage(serverFD, theirAddr, theirAddrLen, message)) <= 0){
        return -1;
    }
    fprintf(stdout, "[%s:%d]: %s\n", inet_ntoa(theirAddr.sin_addr), ntohs(theirAddr.sin_port), message.c_str());
    return 0;
}

/*
 * Listens to the fd; read data and print to terminal
 */
int doServerBehaviour(int fd){
    string message;
    int noBytes;
    struct sockaddr_in theirAddr;
    socklen_t theirAddrLen = sizeof theirAddr;
    fprintf(stdout, "Receiving message from server!!!\n");
    if((noBytes = recieveMessage(fd, theirAddr, theirAddrLen, message)) <= 0){
        fprintf(stderr, "Read %d bytes\n", noBytes);
        return -1;
    }
    fprintf(stdout, "Read %d bytes: %s\n", noBytes, message.c_str());
    return 0;
}

/*
 * Listens to the set of FDs for reading and behaves in the corresponding way
 */
int doProcess(int userFD, nodeinfo nodeInfo){
    int serverFD = nodeInfo.fd;
    struct sockaddr_in destination = nodeInfo.sockAddr;
    int response = 0;
    fd_set readFD;
    int maxFD;

    int retValue = 0;
    while(true){
        FD_ZERO(&readFD);
        maxFD = -1;

        FD_SET(userFD, &readFD);
        maxFD = max(maxFD, userFD);
        FD_SET(serverFD, &readFD);
        maxFD = max(maxFD, serverFD);
        maxFD += 1;
        fprintf(stderr, "Listening...\n");
        retValue = select(maxFD, &readFD, NULL, NULL, NULL);
        if(retValue == -1){
            perror("Select Error: ");
        }
        else if(retValue == 0){
            fprintf(stderr, "No FD is ready.... Still returned Something Wrong!!! Trying again...\n");
        }
        else{
            if(FD_ISSET(userFD, &readFD)){
                // read from user and send to server
                response = doUserBehaviour(userFD, serverFD, destination);
            }
            if(FD_ISSET(serverFD, &readFD)){
                // read from server and display to terminal
                response = doServerBehaviour(serverFD);
            }
        }
        if(response == 1){
            break;
        }
        else if(response == -1){
            return -1;
        }
    }
    return 0;
}

int main(int argc, char *argv[]){
    if (argc != 2) {
        fprintf(stderr, "*** Author: Devanshu Jain (devjain)\n");
        exit(1);
    }

    string serverAddress = argv[1];

    string IPAddress = getIP(serverAddress);
    int port = getPort(serverAddress);

    if(IPAddress == "" || port == -1){
        fprintf(stderr, "[%s] Not the correct format\n", serverAddress.c_str());
        exit(2);
    }

    nodeinfo nodeInfo;
    if(getNodeInfo(IPAddress, port, nodeInfo) != 0){
        exit(2);
    }
    int userFD = STDIN_FILENO;
    doProcess(userFD, nodeInfo);
    close(nodeInfo.fd);

    return 0;
}