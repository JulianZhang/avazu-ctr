-- import Util.Util
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

import Pipes
import System.IO
import Control.Monad (unless)
import Data.Word (Word8)

splitCSVWithColSkip f str = f $ splitCSV str 

splitCSV::BS.ByteString->[BS.ByteString]
splitCSV str = BS.split (BS.c2w ',') str

splitCSV' "" = []
splitCSV' str = [s1] ++ splitCSV'' s2
  where 
  (s1,s2) = break (\x -> ','==x) str

splitCSV'' "" = []
splitCSV'' str = splitCSV' $ tail str


readFileBatch h count readFunc batchFunc = do
  slist <- lift $ readFileBatch' count h []
  yield  $ batchFunc $ map readFunc slist
  let eof = null slist
  unless eof $  readFileBatch h count readFunc batchFunc

readFileBatch'::Int->Handle->[BS.ByteString]->IO [BS.ByteString]
readFileBatch' i h s 
  | i == 0 = do {return s} 
  -- |  lift (hIsEOF h) = do {return s} 
  | otherwise = do
    eof <- hIsEOF h
    case eof of 
      True -> do {return s }
      False -> do  
        str <- BS.fromStrict <$> BSS.hGetLine h
        let newS = str:s 
        let rStr = readFileBatch' (i-1) h newS
        rStr 

-- ===========================

workdir = "workdata/"

batchSize = 2000

readFromPipes::[Handle]->Consumer [BS.ByteString] IO ()
readFromPipes head = do
	body <- await
	lift $ append2HandleLIst  head body
	readFromPipes head


append2HandleLIst handleList dataList = do 
	zipWithM_ (\x y-> BS.hPutStr x y) handleList dataList

append2FileLIst dir fNameList dataList = do
	zipWithM_ (\x y -> appendFile (dir++x ::FilePath) y) fNameList dataList

openFiles::[BS.ByteString]-> IO [Handle]
openFiles fNameList = do
	mapM (\x -> openFile  (workdir++(BSC.unpack x)::FilePath) AppendMode ) fNameList

closeFiles handleList = do
	mapM_ hClose handleList

myBatchFunc ll = map foldLines newll
	where
		newll = transpose ll
		foldLines ls = foldl 
			(\x y -> BS.append (BS.snoc x (BS.c2w '\n') ) y ) 
			(BSC.pack "") ls

main = do 
	s<- getArgs
	let num =  (1+) $ (\x -> div x batchSize)  $ (read . head) s 
	let readCsv = splitCSV 
	withFile "data/train_rev2" ReadMode $ \h -> do 
		csvHead <-BS.fromStrict <$> BSS.hGetLine h 
		handleList <- openFiles (readCsv csvHead)
		runEffect $ 
			readFileBatch h batchSize readCsv  myBatchFunc         -- ( (map unlines) . transpose) 
			>-> P.take num >-> readFromPipes handleList
		closeFiles handleList
--		let outList = zip  (readCsv csvHead)  $ map DM.toList t 
--		mapM (\x -> appendFile "testdata"  ((show x)++ "\n" ) ) outList