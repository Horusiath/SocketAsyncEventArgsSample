using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Threading;

namespace SocketAsyncClient
{
    public sealed class SocketClient : IDisposable
    {
        private int bufferSize = 60000;
        private const int MessageHeaderSize = 4;

        private Socket clientSocket;
        private bool connected = false;
        private IPEndPoint hostEndPoint;
        private AutoResetEvent autoConnectEvent;
        private AutoResetEvent autoSendEvent;
        private SocketAsyncEventArgs sendEventArgs;
        private SocketAsyncEventArgs receiveEventArgs;
        private BlockingCollection<byte[]> sendingQueue;
        private BlockingCollection<byte[]> receivedMessageQueue;
        private Thread sendMessageWorker;
        private Thread processReceivedMessageWorker;

        public SocketClient(IPEndPoint hostEndPoint)
        {
            this.hostEndPoint = hostEndPoint;
            this.autoConnectEvent = new AutoResetEvent(false);
            this.autoSendEvent = new AutoResetEvent(false);
            this.sendingQueue = new BlockingCollection<byte[]>();
            this.receivedMessageQueue = new BlockingCollection<byte[]>();
            this.clientSocket = new Socket(this.hostEndPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            this.sendMessageWorker = new Thread(new ThreadStart(SendQueueMessage));
            this.processReceivedMessageWorker = new Thread(new ThreadStart(ProcessReceivedMessage));

            this.sendEventArgs = new SocketAsyncEventArgs();
            this.sendEventArgs.UserToken = this.clientSocket;
            this.sendEventArgs.RemoteEndPoint = this.hostEndPoint;
            this.sendEventArgs.Completed += new EventHandler<SocketAsyncEventArgs>(OnSend);

            this.receiveEventArgs = new SocketAsyncEventArgs();
            this.receiveEventArgs.UserToken = new AsyncUserToken(clientSocket);
            this.receiveEventArgs.RemoteEndPoint = this.hostEndPoint;
            this.receiveEventArgs.SetBuffer(new Byte[bufferSize], 0, bufferSize);
            this.receiveEventArgs.Completed += new EventHandler<SocketAsyncEventArgs>(OnReceive);
        }

        public void Connect()
        {
            SocketAsyncEventArgs connectArgs = new SocketAsyncEventArgs();
            connectArgs.UserToken = this.clientSocket;
            connectArgs.RemoteEndPoint = this.hostEndPoint;
            connectArgs.Completed += new EventHandler<SocketAsyncEventArgs>(OnConnect);

            clientSocket.ConnectAsync(connectArgs);
            autoConnectEvent.WaitOne();

            SocketError errorCode = connectArgs.SocketError;
            if (errorCode != SocketError.Success)
            {
                throw new SocketException((Int32)errorCode);
            }
            sendMessageWorker.Start();
            processReceivedMessageWorker.Start();

            if (!clientSocket.ReceiveAsync(receiveEventArgs))
            {
                ProcessReceive(receiveEventArgs);
            }
        }

        public void Disconnect()
        {
            clientSocket.Disconnect(false);
        }
        public void Send(byte[] message)
        {
            sendingQueue.Add(message);
        }

        private void OnConnect(object sender, SocketAsyncEventArgs e)
        {
            autoConnectEvent.Set();
            connected = (e.SocketError == SocketError.Success);
        }
        private void OnSend(object sender, SocketAsyncEventArgs e)
        {
            autoSendEvent.Set();
        }
        private void SendQueueMessage()
        {
            while (true)
            {
                var message = sendingQueue.Take();
                if (message != null)
                {
                    sendEventArgs.SetBuffer(message, 0, message.Length);
                    clientSocket.SendAsync(sendEventArgs);
                    autoSendEvent.WaitOne();
                }
            }
        }

        private void OnReceive(object sender, SocketAsyncEventArgs e)
        {
            ProcessReceive(e);
        }
        private void ProcessReceive(SocketAsyncEventArgs e)
        {
            if (e.BytesTransferred > 0 && e.SocketError == SocketError.Success)
            {
                AsyncUserToken token = e.UserToken as AsyncUserToken;

                
                ProcessReceivedData(token.DataStartOffset, token.NextReceiveOffset - token.DataStartOffset + e.BytesTransferred, 0, token, e);

                
                token.NextReceiveOffset += e.BytesTransferred;

                
                if (token.NextReceiveOffset == e.Buffer.Length)
                {
                    
                    token.NextReceiveOffset = 0;

                    
                    if (token.DataStartOffset < e.Buffer.Length)
                    {
                        var notYesProcessDataSize = e.Buffer.Length - token.DataStartOffset;
                        Buffer.BlockCopy(e.Buffer, token.DataStartOffset, e.Buffer, 0, notYesProcessDataSize);

                        
                        token.NextReceiveOffset = notYesProcessDataSize;
                    }

                    token.DataStartOffset = 0;
                }

                
                e.SetBuffer(token.NextReceiveOffset, e.Buffer.Length - token.NextReceiveOffset);

                
                if (!token.Socket.ReceiveAsync(e))
                {
                    ProcessReceive(e);
                }
            }
            else
            {
                ProcessError(e);
            }
        }
        private void ProcessReceivedData(int dataStartOffset, int totalReceivedDataSize, int alreadyProcessedDataSize, AsyncUserToken token, SocketAsyncEventArgs e)
        {
            if (alreadyProcessedDataSize >= totalReceivedDataSize)
            {
                return;
            }

            if (token.MessageSize == null)
            {
                
                if (totalReceivedDataSize > MessageHeaderSize)
                {
                    
                    var headerData = new byte[MessageHeaderSize];
                    Buffer.BlockCopy(e.Buffer, dataStartOffset, headerData, 0, MessageHeaderSize);
                    var messageSize = BitConverter.ToInt32(headerData, 0);

                    token.MessageSize = messageSize;
                    token.DataStartOffset = dataStartOffset + MessageHeaderSize;

                    
                    ProcessReceivedData(token.DataStartOffset, totalReceivedDataSize, alreadyProcessedDataSize + MessageHeaderSize, token, e);
                }
                
                else
                {
                    
                }
            }
            else
            {
                var messageSize = token.MessageSize.Value;
                
                if (totalReceivedDataSize - alreadyProcessedDataSize >= messageSize)
                {
                    var messageData = new byte[messageSize];
                    Buffer.BlockCopy(e.Buffer, dataStartOffset, messageData, 0, messageSize);
                    ProcessMessage(messageData);

                    
                    token.DataStartOffset = dataStartOffset + messageSize;
                    token.MessageSize = null;

                    
                    ProcessReceivedData(token.DataStartOffset, totalReceivedDataSize, alreadyProcessedDataSize + messageSize, token, e);
                }
                
                else
                {
                    
                }
            }
        }
        private void ProcessMessage(byte[] messageData)
        {
            receivedMessageQueue.Add(messageData);
        }
        private void ProcessReceivedMessage()
        {
            while (true)
            {
                var message = receivedMessageQueue.Take();
                if (message != null)
                {
                    var current = Interlocked.Increment(ref Program._receivedMessageCount);
                    if (current == 1)
                    {
                        Program._watch = Stopwatch.StartNew();
                    }
                    if (current % 1000 == 0)
                    {
                        Console.WriteLine("received reply message, length:{0}, count:{1}, timeSpent:{2}", message.Length, current, Program._watch.ElapsedMilliseconds);
                    }
                }
            }
        }

        private void ProcessError(SocketAsyncEventArgs e)
        {
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            

            
            throw new SocketException((Int32)e.SocketError);
        }

        #region IDisposable Members

        public void Dispose()
        {
            autoConnectEvent.Close();
            if (this.clientSocket.Connected)
            {
                this.clientSocket.Close();
            }
        }

        #endregion
    }
}
