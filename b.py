from sys import argv
from time import *
from socket import *
from threading import *
from queue import *
from packet import *

#TODO: Two threads s->d, s<-d

def main(argv):
    # Create a TCP/IP socket
    sock = socket(AF_INET, SOCK_STREAM)

    # Define IP & port number of the server
    IP = ''
    TCP_source_PORT = 10000 # Communicate with source
    UDP_router_PORT = 10001 # Communicate with router
    TCP_addr = (IP, TCP_source_PORT) # Address of B that s will use
    UDP_addr = (IP, UDP_router_PORT) # Address of B that routers will use
    # router_addr1 = ('10.10.2.2', 5010)#link-1 (r1)
    # router_addr2 = ('10.10.4.2', 5010)#link-3 (r2)
    print('Starting TCP server on {} port {}'.format(*TCP_addr))
    # Bind the socket to the port and start listening (at most one connection)
    sock.bind(TCP_addr)
    sock.listen(1)
    #Initialize all variables required
    # worker_thread=Thread(target=, args=(,))
    # worker_thread.start()
    try:
      while True:    
        try:
          # Wait for a connection
          print('Waiting for a connection')
          conn, client_address = sock.accept()
          print('Connection from ip:{} on port number:{}'.format(*client_address))
          connected=True
          while connected:
              #Receive the packet
              packet, address = conn.recvfrom(MAX_PACKET_SIZE)
              if(packet):
                  seq_num=parsePacket(packet)[1]
                  print("Packet with sequence number {} has been successfully received:".format(seq_num))
                  conn.sendall("Retrieved".encode())
                  print("Main thread has sent the response.\n")
              else:
                  print("Connection is over")
                  connected=False
        except ConnectionResetError:
          pass
    finally:
        #Close the connection and the socket
        sock.close()
        print("Socket is closed.")
if __name__ == "__main__":
    main(argv[1:])