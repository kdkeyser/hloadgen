{-# LANGUAGE OverloadedStrings #-}

module Main where

import Network.HTTP.Client
import Network.HTTP.Client.Internal
import Network.HTTP.Types.Status
import qualified Data.ByteString as BS
import Data.Int
import Data.IORef
import qualified Network.Socket as NS
import Control.Monad
import Control.Concurrent

selectEndPoint :: IO (NS.HostAddress, Int)
selectEndPoint = do
  return (NS.tupleToHostAddress (127,0,0,1), 9000)

createConnection :: IO (Maybe NS.HostAddress -> String -> Int -> IO Connection)
createConnection = do
  (endpoint, port) <- selectEndPoint
  return $ \ _ host _ -> openSocketConnection (const $ return ()) (Just endpoint) host port

createManager :: IO Manager
createManager =
  newManager $ defaultManagerSettings 
    { managerRawConnection = createConnection
    , managerIdleConnectionCount = 10
    , managerConnCount = 10
    }
  
requestPopper :: Int64 -> Int -> IO (GivesPopper ()) 
requestPopper objectSize chunkSize = do
  let buffer = BS.replicate chunkSize 0
  state <- newIORef $ StreamFileStatus objectSize 0 chunkSize
  return $ \needsPopper -> needsPopper $ do
    StreamFileStatus objectSize alreadySent chunkSize <- readIORef state
    if alreadySent == objectSize then
      return BS.empty
    else if alreadySent + (fromIntegral chunkSize) <= objectSize then do
      writeIORef state $ StreamFileStatus objectSize (alreadySent + (fromIntegral chunkSize)) chunkSize
      return buffer
    else do
      writeIORef state $ StreamFileStatus objectSize objectSize chunkSize
      return $ BS.replicate (fromIntegral $ objectSize - alreadySent) 0

createRequest :: Int64 -> Int -> IO Request
createRequest objectSize chunkSize = do
   popper <- requestPopper objectSize chunkSize
   return $ defaultRequest
    { method = "PUT"
    , responseTimeout = responseTimeoutNone
    , requestBody = RequestBodyStream objectSize popper
    }

singlePutter :: Manager -> IO () 
singlePutter manager = forever $ do
  request <- createRequest 10000000 (32*1024)
  response <- httpLbs request manager
  return ()
--  putStrLn $ "The status code was: " ++ show (statusCode $ responseStatus response)
   
main :: IO ()
main = do
  manager <- createManager
  threadIds <- sequence $ replicate 10 (forkIO $ singlePutter manager)
  threadDelay 100000000 -- 100 s
  return ()
  
