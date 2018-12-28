from sys import argv
from random import seed, randint
from collections import OrderedDict
from struct import pack, unpack
from time import *
from socket import *
from threading import *
from queue import *
from packet import *

#TODO: b sends special packet to d to mark the indicate the end of the connection

# Define constants
LOCALHOST='127.0.0.1'
R1_IP='10.10.2.2'
R1_PORT=5010
R2_IP='10.10.4.2'
R2_PORT=5010

# End time
end_time = 0
time_lock = Lock()
def setTime(record):
  global end_time
  with time_lock:
    if(record>end_time):
      end_time=record

def parseTime(payload):
  integer=str(int.from_bytes(payload[:MAX_INTEGER], byteorder='little'))
  decimal=str(int.from_bytes(payload[MAX_INTEGER+1:], byteorder='little'))
  epoch=float(integer+"."+decimal)
  return epoch


def router_handler(listen_addr, send_addr, stop, completed, proceed, queue):
  print('[{}]: Listening to port:{}/{}'.format(current_thread().name, *listen_addr))
  sock=socket(AF_INET, SOCK_DGRAM)
  sock.bind(listen_addr)
  sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)# TODO: remove later
  packet_buffer = OrderedDict()
  seq_num = 0
  ack_num = 0
  packets_sent = 0
  all_packets_sent = 0
  sending=True
  completed.acquire()
  try:
    while True:#not stop.is_set():
      base=seq_num
      try:
        # STATE 1: Send all packets in the window
        while(packets_sent<WINDOW_SIZE and sending):
          # Get payload (block if there is no payload to packetize)
          payload = queue.get()
          # If the payload is given as none then the connection is over
          if(payload==None):
            sending=False
            print("[{}]: No new packet".format(current_thread().name))
            print("Packets to be acked:",len(packet_buffer.keys()))
            # Wait for timeouts to send packets that are pending
            break
          # EVENT 1: Send all packets in the window
          # print("[{}]: Event 1: Packet with sequence number: {} sent to {}/{}"
          #       .format(current_thread().name, seq_num, *send_addr)) 
          # Get how many bytes read from file
          payload_size = len(payload)
          # Create packet
          packet = packetize(seq_num, ack_num, payload_size, payload)
          # Find next sequence number
          seq_num = (seq_num + payload_size) % MAX_ALLOWED_SEQ_NUM
          # Keep the expected ack number(same thing as next seq_num) along with the packet just sent 
          packet_buffer[seq_num]=packet
          packets_sent+=1
          all_packets_sent+=1
          print("[{}]: Packets sent: {}".format(current_thread().name, all_packets_sent))
          # Send packet
          sock.sendto(packet, send_addr)
          # For base start the timer
          if seq_num==base:
            sock.settimeout(0.1) # in seconds
        # STATE 2: Polling for for response(s)
        # Get responses
        responses = []
        sock.setblocking(False)
        while True:
          try:
            response=sock.recv(MAX_PACKET_SIZE)
            responses.append(response)
          except BlockingIOError:
            break
        sock.setblocking(True)
        # EVENT 2: Packet recieved
        # print("[{}]: Event 2: Response is received".format(current_thread().name))
        # Parsing and checking for errors
        recent_packet = None
        largest_ack = -1
        for response in responses:
          parsed_response = parsePacket(response)
          if(parsed_response is not None):
            r_seq_num, r_ack_num, r_payload_len, r_payload=parsed_response[1:]
            if(r_ack_num>=largest_ack):
              largest_ack=r_ack_num
              recent_packet=r_payload
        if(largest_ack==-1):
          continue        
        setTime(parseTime(recent_packet))
        # # Correctly in order received, send a response
        # if(ack_num==r_seq_num):
        #   ack_num=(r_seq_num + r_payload_len) % MAX_ALLOWED_SEQ_NUM
        # Expecting cumulative acks
        # If ack is received including the base, disable timeout
        expected_base_ack=next(iter(packet_buffer))
        if(expected_base_ack<=largest_ack):
          sock.settimeout(None)
        else:
          sock.settimeout(0.1) # in seconds
        # Mark packets correctly received (if any) and remove them from buffer
        expected_acks=list(packet_buffer.keys())
        for expected_ack in expected_acks:     
          if(expected_ack<=largest_ack):
            del packet_buffer[expected_ack]
            packets_sent-=1
          else:
            break
        # If packet_buffer is empty all packets are sent, reset state
        if(not packet_buffer):
          print("[{}]: No buffered packet".format(current_thread().name))
          seq_num = 0
          ack_num = 0
          packets_sent = 0
          response=None
          sending=True
          while(response!=EMPTY_PACKET):
            sock.sendto(EMPTY_PACKET, send_addr)
            response=sock.recv(HEADER_SIZE)
          print("[{}]: Transmission completed".format(current_thread().name))
          all_packets_sent=0
          completed.release()
          proceed.acquire()
          completed.acquire()
          proceed.release()
      # EVENT 3: Timeout
      except timeout:
          print("[{}]: Event 3: Timeout triggered, resending the first packet".format(current_thread().name))
          expected_base_ack=next(iter(packet_buffer))
          packet=packet_buffer[expected_base_ack]
          # Send packet
          sock.sendto(packet, send_addr)
          sock.settimeout(0.1)# in seconds
  finally:
    sock.close()
    print("[{}]: Socket is closed.".format(current_thread().name))

def main(argv):
    global R1_IP, R1_PORT, R2_IP, R2_PORT
    # Create a TCP/IP socket
    socketTCP = socket(AF_INET, SOCK_STREAM)
    # Used for selection of routers randomly
    # seed(None)
    # Define IP & port number of the server
    IP=''
    if(len(argv)>0):
      R_IP=argv[0]
      if(R_IP.lower()=="localhost"):
        R1_IP=LOCALHOST
        R2_IP=LOCALHOST
        R1_PORT=5000
        R2_PORT=5001
    # Communicate with source
    TCP_source_PORT = 10000
    # Communicate with router-1
    UDP_router_PORT1 = 10001
    # Communicate with router-2
    UDP_router_PORT2 = 10002 
    # Address of B that s will use
    TCP_addr = (IP, TCP_source_PORT)
    # Addresses of B that router-1 & router-2 will use
    thread_addrs = [(IP, UDP_router_PORT1), (IP, UDP_router_PORT2)]
    # Addresses for link-1 (r1), link-3 (r2)
    router_addrs = [(R1_IP, R1_PORT), (R2_IP, R2_PORT)]
    print('[MAIN THREAD]: Starting TCP server on {} port {}'.format(*TCP_addr))
    # Bind the sockets to the ports
    i=0
    while True:
      try:
        socketTCP.bind((IP, TCP_source_PORT+i))
        print("WARNING: Opened port:", TCP_source_PORT+i)
        break
      except OSError:
        i+=1
    # Enable listening at most one connection
    socketTCP.listen(1)
    # Initialize and start threads
    stop_threads = Event()
    q1=Queue(3000)
    q2=Queue(3000)
    queues=[q1, q2]
    threads_completed=Semaphore(2)
    th1_proceed=Lock()
    th2_proceed=Lock()
    r1_worker_thread=Thread(target=router_handler, args=(thread_addrs[0], router_addrs[0], stop_threads, threads_completed, th1_proceed, q1))
    r2_worker_thread=Thread(target=router_handler, args=(thread_addrs[1], router_addrs[1], stop_threads, threads_completed, th2_proceed, q2))
    r1_worker_thread.start()
    r2_worker_thread.start()
    connection_socket = None
    try:
      while True:
        try:
          th1_proceed.acquire()
          th2_proceed.acquire()    
          # Wait for a connection
          print('[MAIN THREAD]: Waiting for a connection...')
          # New TCP socket is opened and named as "connection_socket"
          connection_socket, client_address = socketTCP.accept()
          print('[MAIN THREAD]: Connection from ip:{} on port number:{}'.format(*client_address))
          packets_forwarded=0
          connected=True
          while connected:
            #Receive the packet
            payload, address = connection_socket.recvfrom(PAYLOAD_SIZE)
            # Send packet via one of the threads
            selection=packets_forwarded%2 # TODO: Use randint later 
            queues[selection].put_nowait(payload)
            packets_forwarded+=1
            if(len(payload)!=PAYLOAD_SIZE):
                queues[0].put_nowait(None)
                queues[1].put_nowait(None)
                print("[MAIN THREAD]: {} packets forwarded. Waiting for both threads to complete...".format(packets_forwarded))
                threads_completed.acquire()
                th1_proceed.release()
                th2_proceed.release()
                threads_completed.release()
                connected=False
                connection_socket.sendall(pack('d', end_time))
                print("[MAIN THREAD]: Connection is over")
                print("[MAIN THREAD]: End time of upload:", end_time)
        finally:
          connection_socket.close()
          print("[MAIN THREAD]: Connection socket is closed.")
    finally:
        if(connection_socket):
          connection_socket.close()
        # Close the socket
        socketTCP.close()
        r1_worker_thread.join()
        r2_worker_thread.join()
        print("[MAIN THREAD]: Socket is closed.")
if __name__ == "__main__":
    main(argv[1:])