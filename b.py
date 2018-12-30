from sys import argv
from random import seed, randint
from collections import OrderedDict
from struct import pack, unpack
from time import *
from socket import *
from threading import *
from queue import *
from packet import *

# TODO: b prepares packets and sequence numbers which ease the tasks by threads

# Define constants
LOCALHOST='127.0.0.1'
R1_IP='10.10.2.2'
R1_PORT=5010
R2_IP='10.10.4.2'
R2_PORT=5010

# Shared sequence and ack numbers
# packet_buffer = OrderedDict()
# shared_buffer_lock = Lock()
# ack_num=0
# largest_ack_obtained=0
# shared_number_lock = Lock()
# shared_largest_ack_lock=Lock()
# Start and end time of upload
start_time = 0
end_time = 0
shared_time_lock = Lock()

# def printState():
#   keys=list(packet_buffer.keys())
#   print("[{}]: Packet buffer keys: {}, length:{}".format(current_thread().name,keys,len(keys)))
#   print("[{}]: Largest ACK number: {}".format(current_thread().name,largest_ack_obtained))
#   print("[{}]: Start time: {}".format(current_thread().name,start_time))
#   print("[{}]: End time: {}".format(current_thread().name,end_time))


def setTime(record):
  global end_time
  if(record):
    with shared_time_lock:
      if(record>end_time):
        end_time=record

# def setReceivedACK(ack):
#   global largest_ack_obtained
#   with shared_largest_ack_lock:
#     if(ack>largest_ack_obtained):
#       largest_ack_obtained=ack

def parseTime(payload):
  if(payload):
    integer=str(int.from_bytes(payload[:MAX_INTEGER], byteorder='little'))
    decimal=str(int.from_bytes(payload[MAX_INTEGER+1:], byteorder='little'))
    epoch=float(integer+"."+decimal)
    return epoch
  return None


def router_handler(listen_addr, send_addr, stop, completed, queue):
  global ack_num
  print('[{}]: Listening to port:{}/{}'.format(current_thread().name, *listen_addr))
  sock=socket(AF_INET, SOCK_DGRAM)
  sock.bind(listen_addr)
  sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)# TODO: remove later
  # ack_num = 0
  packet_buffer=OrderedDict()
  packetsSent = 0
  allPacketsSent = 0
  queueNotEmpty=True
  keepSending=True
  isFirstPacket=True
  # base_seq=0
  # next_window_seq=0
  # expected_base_ack=0
  # try:
  while True:#not stop.is_set():
    try:
      # STATE 1: Send all packets in the window
      while(keepSending and packetsSent<WINDOW_SIZE and queueNotEmpty):
        # Get payload (block if there is no payload to packetize)
        seq_num, expected_ack_num, packet = queue.get()
        # next_window_seq = seq_num
        # If the payload is given as none then the connection is over
        if(packet==None):
          queueNotEmpty=False
          break
        # For base start the timer
        # if isFirstPacket:
          # sock.settimeout(0.1) # in seconds
          # base_seq=seq_num
          # expected_base_ack=expected_ack_num
          # isFirstPacket=False
          # print("[{}]: No new packet".format(current_thread().name))
          # print("Packets to be acked:",len(packet_buffer.keys()))
          # Wait for timeouts to send packets that are pending
        # EVENT 1: Send all packets in the window
        # print("[{}]: Event 1: Packet with sequence number: {} will be send to {}/{}"
              # .format(current_thread().name, seq_num, *send_addr)) 
        packetsSent+=1
        allPacketsSent+=1
        # print("[{}]: Packets sent: {}".format(current_thread().name, allPacketsSent))
        # Send packet
        sock.sendto(packet, send_addr)
        packet_buffer[expected_ack_num]=(seq_num, packet)
      # STATE 2: Polling for for response(s)
      # print("[{}]: Packets sent: {}".format(current_thread().name, allPacketsSent))
      # sock.settimeout(0.1)
      keepSending=False
      if(queueNotEmpty or packet_buffer):
        print("[{}]: Waiting for a response...".format(current_thread().name))
        response=sock.recv(MAX_PACKET_SIZE)
        parsed_response = parsePacket(response)
        if(parsed_response is None):
          continue
        r_seq_num, r_ack_num, r_payload_len, r_payload=parsed_response[1:]
        # setReceivedACK(r_ack_num)       
        setTime(parseTime(r_payload))
        # Mark packets correctly received (if any) and remove them from buffer
        # Expecting cumulative acks
        # If ack is received including the base, disable timeout
        expected_base_ack=next(iter(packet_buffer))
        print("[{}]: Base seq: {}, Expected ACK num: {}".format(current_thread().name, packet_buffer[expected_base_ack][0], expected_base_ack))
        print("[{}]: Parsed ACK num: {}".format(current_thread().name, r_ack_num))
        if(expected_base_ack==r_ack_num):
          # sock.settimeout(None)
          # isFirstPacket=True
          del packet_buffer[r_ack_num]
          packetsSent-=1
          if(queueNotEmpty):
            keepSending=True
        elif(r_ack_num in packet_buffer):
          del packet_buffer[r_ack_num]
          packetsSent-=1
      # If packet_buffer is empty all packets are sent, reset state
      if(not queueNotEmpty and not packet_buffer):
        # print("[{}]: No buffered packet".format(current_thread().name))
        while(True):
          try:
            sock.sendto(EMPTY_PACKET, send_addr)
            # sock.settimeout(0.1) # in seconds
            # print("[{}]: Waiting an empty packet".format(current_thread().name))
            response=sock.recv(HEADER_SIZE)
            if(response==EMPTY_PACKET):
              break
          except timeout:
            pass
        print("[{}]: Transmission completed".format(current_thread().name))
        print("[{}]: Before barrier".format(current_thread().name))
        completed.wait()
        packetsSent = 0
        allPacketsSent = 0
        queueNotEmpty=True
        keepSending=True
        isFirstPacket=True
        base_seq=0
        next_window_seq=0
        expected_base_ack=0
        print("[{}]: After barrier".format(current_thread().name))
    # EVENT 3: Timeout
    except timeout:
        expected_base_ack=next(iter(packet_buffer))
        print("[{}]: Event 3: Timeout triggered, resending the packet due to base_ack: {}".format(current_thread().name, expected_base_ack))
        packet=packet_buffer[expected_base_ack][1]
        # Send packet
        sock.sendto(packet, send_addr)
        # sock.settimeout(0.1)# in seconds
  #     except Exception as e:
  #       print("[{}]: EXCEPTION1: {}".format(current_thread().name, e))
  # except Exception as e:
  #   print("EXCEPTION2:",e)
  # finally:
  #   sock.close()
  #   print("[{}]: Socket is closed.".format(current_thread().name))

def main(argv):
    global R1_IP, R1_PORT, R2_IP, R2_PORT, start_time, end_time
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
    threads_completed=Barrier(3)
    r1_worker_thread=Thread(target=router_handler, args=(thread_addrs[0], router_addrs[0], stop_threads, threads_completed, q1))
    r2_worker_thread=Thread(target=router_handler, args=(thread_addrs[1], router_addrs[1], stop_threads, threads_completed, q2))
    r1_worker_thread.start()
    r2_worker_thread.start()
    connection_socket = None
    seq_num=0
    ack_num=0
    try:
      while True:
        try:
          # Wait for a connection
          print('[MAIN THREAD]: Waiting for a connection...')
          # New TCP socket is opened and named as "connection_socket"
          connection_socket, client_address = socketTCP.accept()
          start_time=time()
          print('[MAIN THREAD]: Connection from ip:{} on port number:{}'.format(*client_address))
          seq_num=0
          packets_forwarded=0
          connected=True
          while connected:
            #Receive the packet
            payload, address = connection_socket.recvfrom(PAYLOAD_SIZE)
            # Get how many bytes read from file
            payload_size = len(payload)
            if(payload_size==0):
                queues[0].put_nowait((None, None, None))
                queues[1].put_nowait((None, None, None))
                print("[MAIN THREAD]: {} packets forwarded. Waiting for both threads to complete...".format(packets_forwarded))
                threads_completed.wait()
                threads_completed.reset()
                print("[MAIN THREAD]: Start time of upload:", start_time)
                print("[MAIN THREAD]: End time of upload:", end_time)
                print("[MAIN THREAD]: Duration of upload:", end_time-start_time)
                connection_socket.sendall(pack('d', end_time))
                start_time=0
                end_time=0
                seq_num=0
                # ack_num=0
                # largest_ack_obtained=0
                connected=False
                # printState()
            else:
                # Create packet
                packet = packetize(seq_num, ack_num, payload_size, payload)
                # Keep the expected ack number(same thing as next seq_num) along with the packet just sent 
                # packet_buffer[seq_num]=packet
                # Find next sequence number
                expected_ack = seq_num + payload_size
                # Send packet via one of the threads
                selection=packets_forwarded%2 # TODO: Use randint later 
                queues[selection].put_nowait((seq_num, expected_ack, packet))
                seq_num = expected_ack
                packets_forwarded+=1
        except Exception as e:
          print("In MAIN:", e)
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