import Util.Util
import System.IO

import System.Environment

import Pipes
import qualified Pipes.Prelude as P

import qualified Data.Map as DM
import Control.Monad
import Control.Applicative
import Data.List

import qualified Data.ByteString as BSS (hGetLine)

import qualified Data.ByteString.Lazy as BS
import qualified Data.ByteString.Internal as BS (c2w,w2c)
import qualified Data.ByteString.Lazy.Char8 as BSC

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

emptyMap = DM.empty::(DM.Map (String,String) Int)

keyCount num = do
	let parStrList = map BSC.pack [
			"C1"
			--,"C22","banner_pos" ,"device_make"
			--,"C17","C23","device_model","C18"
			--,"C24","device_conn_type","device_os","site_category"
			--,"C19","app_category","device_geo_country","device_type"
			--,"site_domain","C20","app_domain","device_id","h_date"
			--,"site_id","C21","app_id","h_hour"
			--,"device_ip"
			]
	let flag = BSC.pack "_count"
	readHandleList <- openFiles "dataByColumn/" parStrList ReadMode

	writeHadleList <- openFilesAppend (map (\x -> BS.append x flag) parStrList)
	let handPairList = zip readHandleList writeHadleList
	clickList <- fmap lines $ readFile "dataByColumn/click"
	mapM_ (\(x,y) -> rCount num clickList x y) handPairList
	closeFiles writeHadleList
	closeFiles readHandleList

mapToString::DM.Map (String,String) Int-> String
mapToString m = unlines $ map eachItem itemList
	where 
		itemList = DM.toList m
		eachItem ((x,y),i) = show x ++ "," ++ show y ++ "," ++ show i 

rCount::Int -> [String] -> Handle->Handle -> IO()
rCount num resultList readHandle writeHadle = do 
	-- rP <- each resultList
	-- keyP <- P.fromHandle readHandle
	rt <- P.fold (\x y -> DM.unionWith (+) x y) emptyMap id $  P.zipWith (\x y -> DM.singleton (x,y) 1) (P.fromHandle readHandle) (each resultList) >-> P.take num
	hPutStr writeHadle $ mapToString  rt
	--hPutStr  writeHadle $ show rt
	
	--P.fold (DM.unionWith (+)) emptyMap id $
	--return zipP
	 

main = do 
	s<- getArgs
	let num =  (1+) $ (\x -> div x batchSize)  $ (read . head) s 
	keyCount num

