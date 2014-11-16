import Util.Util
import System.IO

import System.Environment

import Pipes
import qualified Pipes.Prelude as P

import qualified Data.Map.Strict as DM
import Control.Monad
import Control.Applicative
import Data.List

import qualified Data.ByteString as BSS (hGetLine)

import qualified Data.ByteString.Lazy as BS
import qualified Data.ByteString.Internal as BS (c2w,w2c)
import qualified Data.ByteString.Lazy.Char8 as BSC
import qualified Data.Sequence as Vec
import qualified Data.Traversable as DF

workdir = "workdata/"

batchSize = 1

readFromPipes::[Handle]->Consumer [BS.ByteString] IO ()
readFromPipes head = do
  body <- await
  lift $ append2HandleLIst  head body
  readFromPipes head


append2HandleLIst handleList dataList = do 
  zipWithM_ (\x y-> BS.hPutStr x y) handleList dataList

append2FileLIst dir fNameList dataList = do
  zipWithM_ (\x y -> appendFile (dir++x ::FilePath) y) fNameList dataList




openFilesAppend fNameList  = openFiles workdir fNameList AppendMode
  


openFiles dir fNameList  mode = do
  mapM (\x -> openFile  (dir ++(BSC.unpack x)::FilePath) mode  ) fNameList

closeFiles handleList = do
  mapM_ hClose handleList

newline = BS.c2w '\n'

myBatchFunc ll = map foldLines newll
  where
    newll = transpose ll
    foldLines ls = foldl 
      (\x y -> BS.append  x (BS.cons newline y) ) 
      (BSC.pack "") ls

singleBatch ls = map (\x -> BS.cons newline x) $ head ls 

saveCsvToColFile num = do
  let readCsv = splitCSV 
  withFile "data/train_rev2" ReadMode $ \h -> do 
    csvHead <-BS.fromStrict <$> BSS.hGetLine h 
    handleList <- openFilesAppend (readCsv csvHead)
    runEffect $ 
      readFileBatch h batchSize readCsv  singleBatch         -- ( (map unlines) . transpose) 
      >-> P.take num >-> readFromPipes handleList
    closeFiles handleList

getHour bStr =  (\(x,y)->[x,y]) $ BS.splitAt 6 bStr

splitHour num = do
  withFile "dataByColumn/hour" ReadMode $ \h -> do
    handleList <- openFilesAppend $ map BSC.pack ["h_date","h_hour"]
    runEffect $ readFileBatch h batchSize getHour singleBatch
      >-> P.take num >-> readFromPipes handleList
    closeFiles handleList

emptyMap = DM.empty::(DM.Map (BS.ByteString,BS.ByteString) Int)

keyCount num = do
  let parStrList = map BSC.pack ["C1","C22","banner_pos","device_make"]
      --,"C17","C23","device_model","C18"
      --,"C24","device_conn_type","device_os","site_category"
      --,"C19","app_category","device_geo_country","device_type"
      --,"site_domain","C20","app_domain","device_id","h_date"
      --,"site_id","C21","app_id","h_hour"
      --,"device_ip"
      --]
  let flag = BSC.pack "_count"
  readHandleList <- fmap Vec.fromList $ openFiles "dataByColumn/" parStrList ReadMode
  writeHadleList <- fmap Vec.fromList  $ openFilesAppend (map (\x -> BS.append x flag) parStrList)
  --let handPairList = zip readHandleList writeHadleList
  clickList <- openFile "dataByColumn/click" ReadMode
  out <- DF.sequence $ Vec.zipWith (rCount num clickList) readHandleList  writeHadleList
  --closeFiles writeHadleList
  --closeFiles readHandleList
  return out


--mapToString::DM.Map (String,String) Int-> String
mapToString m = unlines $ map eachItem itemList
  where 
    itemList = DM.toList m
    eachItem ((x,y),i) = show x ++ "," ++ show y ++ "," ++ show i 

pipesCount  n = do
  let ct = 10000
  replicateM_  ct $ do                     -- Repeat this block 'n' times
          x <- await                         -- 'await' a value of type 'a'
          yield x                            -- 'yield' a value of type 'a'
  lift $ putStrLn $ (show (n*ct) )  ++ "rows done!" 
  pipesCount (n+1)

genP::Handle->Producer BS.ByteString IO ()
genP h =  readFileBatch h 1 id head 

--rCount::Int -> [String] -> Handle->Handle -> IO()
rCount num resultList readHandle writeHadle = do 
  -- rP <- each resultList
  -- keyP <- P.fromHandle readHandle
  rt <- P.fold (\x y -> DM.unionWith (+) x y) emptyMap id $  P.zipWith (\x y -> DM.singleton (x,y) 1) (genP readHandle) (genP resultList) >-> P.take num >-> pipesCount 1
  hPutStr writeHadle $ mapToString  rt
  --hPutStr  writeHadle $ show rt
  
  --P.fold (DM.unionWith (+)) emptyMap id $
  --return zipP
   

main = do 
  s<- getArgs
  let num =  (1+) $ (\x -> div x batchSize)  $ (read . head) s 
  keyCount num

