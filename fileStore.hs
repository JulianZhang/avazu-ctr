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





-- ===========================

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

openFiles::[BS.ByteString]-> IO [Handle]
openFiles fNameList = do
	mapM (\x -> openFile  (workdir++(BSC.unpack x)::FilePath) AppendMode ) fNameList

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


main = do 
	s<- getArgs
	let num =  (1+) $ (\x -> div x batchSize)  $ (read . head) s 
	let readCsv = splitCSV 
	withFile "data/train_rev2" ReadMode $ \h -> do 
		csvHead <-BS.fromStrict <$> BSS.hGetLine h 
		handleList <- openFiles (readCsv csvHead)
		runEffect $ 
			readFileBatch h batchSize readCsv  singleBatch         -- ( (map unlines) . transpose) 
			>-> P.take num >-> readFromPipes handleList
		closeFiles handleList
--		let outList = zip  (readCsv csvHead)  $ map DM.toList t 
--		mapM (\x -> appendFile "testdata"  ((show x)++ "\n" ) ) outList