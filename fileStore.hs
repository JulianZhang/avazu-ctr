import Util.Util
import System.IO

import System.Environment

import Pipes
import qualified Pipes.Prelude as P

import qualified Data.Map as DM
import Control.Monad
import Data.List



workdir = "workdata/"

batchSize = 2000

readFromPipes::[Handle]->Consumer [String] IO ()
readFromPipes head = do
	body <- await
	lift $ append2HandleLIst  head body
	readFromPipes head


append2HandleLIst handleList dataList = do 
	zipWithM_ (\x y-> hPutStr x y) handleList dataList

append2FileLIst dir fNameList dataList = do
	zipWithM_ (\x y -> appendFile (dir++x ::FilePath) y) fNameList dataList

openFiles fNameList = do
	mapM (\x -> openFile  (workdir++x::FilePath) AppendMode ) fNameList

closeFiles handleList = do
	mapM_ hClose handleList

main = do 
	s<- getArgs
	let num =  (1+) $ (\x -> div x batchSize)  $ (read . head) s 
	let readCsv = splitCSV 
	withFile "data/train_rev2" ReadMode $ \h -> do 
		csvHead <- hGetLine h 
		handleList <- openFiles (readCsv csvHead)
		runEffect $ 
			readFileBatch h batchSize readCsv  ( (map unlines) . transpose) 
			>-> P.take num >-> readFromPipes handleList
		closeFiles handleList
--		let outList = zip  (readCsv csvHead)  $ map DM.toList t 
--		mapM (\x -> appendFile "testdata"  ((show x)++ "\n" ) ) outList