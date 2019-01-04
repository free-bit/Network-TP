from sys import argv
from time import *
from socket import *
from threading import *
from queue import *
from signal import *
from packet import *
from ntp import *

# TODO: create a proper indexing mechanism for packets sent

# Define constants
LOCALHOST='127.0.0.1'
D_TH1_IP='10.10.3.2'
D_TH1_PORT=5000
D_TH2_IP='10.10.5.2'
D_TH2_PORT=5001
NTP_OFFSET=0
SOURCE_SMALL_PACKET_SIZE=3
# Define max number of pipelining
WINDOW_SIZE=30
# Start and end time of upload
start_time = 0
end_time = 0
shared_time_lock = Lock()

EMPTY_LIST=[None]*WINDOW_SIZE
# Timer handler is called in every TIMER_PERIOD seconds
TIMER_PERIOD = 0.25
# Timeout takes place every "TIMEOUT_PERIOD"th enterance to the handler 
TIMEOUT_PERIOD = 3
# (TIMER_PERIOD*TIMEOUT_PERIOD seconds is the timeout value)
interrupt_count=0
sockets=None
buffers=None
timers=None
send_addrs=None

# Try to update the timer, if it is not set do nothing (Called only by main thread)
def tick(thread_id, index):
  global timers
  try:
    timers[thread_id][index]-=1
  except TypeError:
    pass

# Send packet in the window, used for resending the packet (Called only by main thread)
def sendPacketFromWindow(thread_id, index):
  global sockets, buffers, send_addrs
  try:
    sockets[thread_id].sendto(buffers[thread_id][index], send_addrs[thread_id])
    timers[thread_id][index]=TIMEOUT_PERIOD
    print("[{}]: Retransmission of a packet".format(current_thread().name))
  # The acknowledgment has just received
  except TypeError:
    pass

# When one TIMER_PERIOD is passed, update all active timers and resend timed-out packets (Called only by main thread)
def tickAll(thread_id):
  global WINDOW_SIZE, timers
  for i in range(WINDOW_SIZE):
    tick(thread_id, i)
    if(timers[thread_id][i]==0):
      sendPacketFromWindow(thread_id, i)

# Only the main thread enters the handler
def timerHandler(signum, _):
  # global interrupt_count
  # Update thread-1 timer context, take action if time is out
  tickAll(0)
  # Update thread-2 timer context, take action if time is out
  tickAll(1)
  # interrupt_count+=1

# Parses timestamp information in payload of the message
def parseTime(payload):
  if(payload):
    integer=str(int.from_bytes(payload[:MAX_INTEGER], byteorder='little'))
    decimal=str(int.from_bytes(payload[MAX_INTEGER+1:], byteorder='little'))
    epoch=float(integer+"."+decimal)
    return epoch
  return None

# Used for updating file transfer time
def setTime(record):
  global end_time
  if(record):
    with shared_time_lock:
      if(record>end_time):
        end_time=record
# Broker worker threads
class BrokerThread(Thread):
  def __init__(self, self_id, barrier, queue):
    global sockets, buffers, timers, send_addrs
    super(BrokerThread, self).__init__()
    # Using shared global scope variables
    self.sock = sockets[self_id]
    self.packets = buffers[self_id]
    self.timers = timers[self_id]
    self.send_addr = send_addrs[self_id]
    # Provided by main
    self.barrier=barrier
    self.shared_queue=queue
    # Thread local context
    # Possible expected ack values: 
      # None: No ACK is expected
      # True: ACK is already received,
      # Float: Specified ACK is expected
    self.expected_acks=EMPTY_LIST[:]
    self.windowBase = 0
    self.windowNext = WINDOW_SIZE-1
    self.isFull=False

  def isWindowFull(self):
    return self.isFull 

  def isWindowEmpty(self):
    return (not self.isFull) and ((self.windowNext+1) % WINDOW_SIZE == self.windowBase)

  def setSocket(self, sock):
    self.sock=sock

  def setSendAddr(self, addr):
    self.send_addr=addr

  def getSocket(self):
    return self.sock

  def getBoundAddress(self):
    return self.sock.getsockname()

  # Set/start timeout for the packet with provided index (SHARED with main thread)
  def startTimer(self, index=None):
    if(not index):
      index=self.windowNext
    self.timers[index]=TIMEOUT_PERIOD

  # Disable timeout for the packet with provided index (Not shared but timer value changes)
  def stopTimer(self, index):
    self.timers[index]=None

  # Send the packet
  def sendPacket(self, packet, send_addr=None):
    if(not send_addr):
      send_addr=self.send_addr
    self.sock.sendto(packet, send_addr)

  # Enqueue
  def enqueue(self, expected_ack, packet):
    if(not self.isWindowFull()):
      self.windowNext=(self.windowNext+1) % WINDOW_SIZE
      self.expected_acks[self.windowNext]=expected_ack
      self.packets[self.windowNext]=packet
      if(self.windowNext+1) % WINDOW_SIZE == self.windowBase: 
        self.isFull = True
    else:
      raise Exception("Enqueue to a full queue!")

  # Dequeue and stop timer
  def dequeue(self):
    if(not self.isWindowEmpty()):
      self.expected_acks[self.windowBase]=None
      self.packets[self.windowBase]=None
      self.stopTimer(self.windowBase) 
      self.windowBase = (self.windowBase+1) % WINDOW_SIZE
      self.isFull = False
    else:
      raise Exception("Dequeue from an empty queue!")

  # Receive a packet from the server, parse, if no error return parsed
  def getResponse(self, size=MAX_PACKET_SIZE):
    response=self.sock.recv(size)
    # Parsing operation performs checksum validation
    parsed_response = parsePacket(response)
    if(parsed_response is None):
      return None
    # Return values other than checksum
    return parsed_response[1:]

  # Receive the empty packet from the server, if incoming packet corrupted, discard
  def getEmptyPacket(self, size=HEADER_SIZE):
    response=self.sock.recv(size)
    if(response==EMPTY_PACKET):
      return True
    return False

  # Find new base i.e. first unACKed packet
  def getFirstUnACKed(self):
    global WINDOW_SIZE
    count=0
    new_base=self.windowBase
    while self.expected_acks[new_base]==True and count<WINDOW_SIZE:
      new_base=(new_base+1)%WINDOW_SIZE
      count+=1
    return count
  # Slide the window and update the base
  # When sliding all packets before the first unACKed packet will be removed
  def slideWindow(self):
    number_of_slides=self.getFirstUnACKed()
    while number_of_slides:
      self.dequeue()
      number_of_slides-=1

  # Mark packet with given ACK
  # If ack is for the base, then slide the window
  def getACK(self, ack):
    try:
      index=self.expected_acks.index(ack)
    except ValueError:
      return
    self.stopTimer(index)
    self.expected_acks[index]=True
    if(index==self.windowBase):
      self.slideWindow()

  # Reset state to initial configuration
  def reset(self):
    self.packets[:]       = EMPTY_LIST
    self.timers[:]        = EMPTY_LIST
    self.expected_acks[:] = EMPTY_LIST
    self.windowBase       = 0
    self.windowNext       = WINDOW_SIZE-1
    self.isFull           = False

  # Thread loop
  def run(self):
    global ack_num
    print('[{}]: Listening to port:{}/{}'.format(self.getName(), *self.getBoundAddress()))
    allPacketsSent = 0
    # Flag for shared_queue
    packetsToSend=True
    # try:
    while True:
      # try:
        # STATE 1: Send all packets in the window
        while(not self.isWindowFull() and packetsToSend):
          # Get payload (block if there is no payload to packetize)
          seq_num, expected_ack_num, packet = self.shared_queue.get()
          # If the payload is given as none then the connection is over
          if(packet==None):
            packetsToSend=False
            break
          # Send packet to destination, store in buffer and start the timer
          self.sendPacket(packet)
          self.enqueue(expected_ack_num, packet)
          self.startTimer()
          allPacketsSent+=1
        # STATE 2: Polling for for response(s)
        # print("[{}]: Packets sent: {}".format(current_thread().name, allPacketsSent))
        if(packetsToSend or not self.isWindowEmpty()):
          # print("[{}]: Waiting for a response...".format(self.getName()))
          parsed_response = self.getResponse()
          if(parsed_response is None):
            continue
          # Extract information from the packet
          r_seq_num, r_ack_num, r_payload_len, r_payload=parsed_response
          print("[{}]: Retrieved ACK: {}".format(self.getName(), r_ack_num))
          # Update time
          setTime(parseTime(r_payload))
          # Mark the packet correctly received and stop its timer
          self.getACK(r_ack_num)
        # If packet_buffer is empty all packets are sent, reset state
        if(not packetsToSend and self.isWindowEmpty()):
          # print("[{}]: No buffered packet".format(current_thread().name))
          while(True):
            self.sendPacket(EMPTY_PACKET)
            self.enqueue(0, EMPTY_PACKET)
            self.startTimer()
            print("[{}]: Waiting an empty packet...".format(current_thread().name))
            isEmpty = self.getEmptyPacket()
            if(isEmpty):
              break
          print("[{}]: Transmission completed, waiting for other thread...".format(current_thread().name))
          self.reset()
          self.barrier.wait()
          packetsToSend=True
          # print("[{}]: State is reset.".format(current_thread().name))
      # except Exception as e:
      #   print("[{}]: EXCEPTION1: {}".format(current_thread().name, e))

def main(argv):
    global NTP_OFFSET,interrupt_count,\
           D_TH1_IP, D_TH1_PORT,\
           D_TH2_IP, D_TH2_PORT,\
           start_time, end_time,\
           sockets, buffers,\
           timers, send_addrs
    # timerHandler is bound to SIGALRM interrupt to create timer interrupts
    signal(SIGALRM, timerHandler)
    # Create a TCP/IP socket
    socketTCP = socket(AF_INET, SOCK_STREAM)
    # Define IP & port number of the server
    B_MAIN_IP='10.10.1.2'
    B_TH1_IP='10.10.2.1'
    B_TH2_IP='10.10.4.1'
    argc=len(argv)
    if(argc>0):
      # Run at localhost for testing purposes
      if("--localhost" in argv):
        B_MAIN_IP=LOCALHOST
        B_TH1_IP=LOCALHOST
        B_TH2_IP=LOCALHOST
        D_TH1_IP=LOCALHOST
        D_TH2_IP=LOCALHOST
      if("--ntp" in argv):
        NTP_OFFSET=getNTPTime()
    # Communicate with source
    TCP_source_PORT = 10000
    # Communicate with router-1
    UDP_router_PORT1 = 10001
    # Communicate with router-2
    UDP_router_PORT2 = 10002 
    # Address of B that s will use
    TCP_addr = [B_MAIN_IP, TCP_source_PORT]
    # Addresses of B that router-1 & router-2 will use
    b_th_addrs = [(B_TH1_IP, UDP_router_PORT1), (B_TH2_IP, UDP_router_PORT2)]
    # Addresses for link-1 (r1), link-3 (r2)
    send_addrs = [(D_TH1_IP, D_TH1_PORT), (D_TH2_IP, D_TH2_PORT)]
    # Bind the sockets to the ports
    i=0
    while True:
      try:
        socketTCP.bind((B_MAIN_IP, TCP_source_PORT+i))
        TCP_addr[1]=TCP_source_PORT+i
        break
      except OSError:
        print("WARNING: Port couldn't be opened. Probing for an available port...")
        i+=1
    print('[MAIN THREAD]: Starting TCP server on {} port {}'.format(*TCP_addr))
    # Enable listening at most one connection
    socketTCP.listen(1)
    # Initialize and start threads
    sock1=socket(AF_INET, SOCK_DGRAM)
    sock2=socket(AF_INET, SOCK_DGRAM)
    sock1.bind(b_th_addrs[0])
    sock2.bind(b_th_addrs[1])
    sockets=[sock1, sock2]
    buffers=[EMPTY_LIST[:], EMPTY_LIST[:]]
    timers=[EMPTY_LIST[:], EMPTY_LIST[:]]
    queues=[Queue(3000), Queue(3000)]
    barrier=Barrier(3)
    r1_worker_thread=BrokerThread(0, barrier, queues[0])
    r2_worker_thread=BrokerThread(1, barrier, queues[1])
    r1_worker_thread.start()
    r2_worker_thread.start()
    connection_socket = None
    seq_num=0
    ack_num=0
    # try:
    while True:
      # try:
        # Wait for a connection
        print('[MAIN THREAD]: Waiting for a connection...')
        # New TCP socket is opened and named as "connection_socket"
        connection_socket, client_address = socketTCP.accept()
        start_time=time()-NTP_OFFSET
        print('[MAIN THREAD]: Connection from ip:{} on port number:{}'.format(*client_address))
        # Decrement interval timer in real time, and deliver SIGALRM upon expiration.
        # Start timer after 50 ms
        # Call handler in every TIMER_PERIOD seconds
        setitimer(ITIMER_REAL, 0.05, TIMER_PERIOD)
        seq_num=0
        packets_forwarded=0
        while True:
          # Receive the message size/boundary for the packet first
          initialBytesToRead = SOURCE_SMALL_PACKET_SIZE
          bytesToRead = bytes()
          while(initialBytesToRead):
            read = connection_socket.recv(initialBytesToRead)
            if(len(read)==0):
              break
            initialBytesToRead -= len(read)
            bytesToRead += read
          # Connection with s is over
          if(len(bytesToRead)==0):
              queues[0].put_nowait((None, None, None))
              queues[1].put_nowait((None, None, None))
              print("[MAIN THREAD]: {} packets forwarded. Waiting for both threads to complete...".format(packets_forwarded))
              # print("[MAIN THREAD]: Total number of bytes received from the source:",bytes_received_in_total) 
              barrier.wait()
              barrier.reset()
              setitimer(ITIMER_REAL, 0, 0)
              # print("[MAIN THREAD]: Start time of upload:", start_time)
              # print("[MAIN THREAD]: End time of upload:", end_time)
              print("[MAIN THREAD]: Duration of upload:", end_time-start_time)
              start_time=0
              end_time=0
              seq_num=0
              break
          # Reading payload with respect to size defined in the earlier message
          payload_size = bytesToRead = int.from_bytes(bytesToRead, byteorder='little')
          payload = bytes()
          while(bytesToRead):
            read = connection_socket.recv(bytesToRead)
            bytesToRead -= len(read)
            payload += read
          # Create packet
          packet = packetize(seq_num, ack_num, payload_size, payload)
          # Find next sequence number
          expected_ack = seq_num + payload_size
          # Send packet via one of the threads
          selection=packets_forwarded%2
          queues[selection].put_nowait((seq_num, expected_ack, packet))
          seq_num = expected_ack
          packets_forwarded+=1
    #     except Exception as e:
    #       print("In MAIN:", e)
    #     finally:
    #       connection_socket.close()
    #       print("[MAIN THREAD]: Connection socket is closed.")
    # finally:
    #     if(connection_socket):
    #       connection_socket.close()
    #     # Close the sockets
    #     socketTCP.close()
    #     r1_worker_thread.join()
    #     r2_worker_thread.join()
    #     sock1.close()
    #     sock2.close()
    #     print("[MAIN THREAD]: Sockets are closed.")
if __name__ == "__main__":
    main(argv[1:])