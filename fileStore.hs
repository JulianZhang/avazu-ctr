import Util.Util
import System.IO

import System.Environment

import Pipes
import qualified Pipes.Prelude as P

import qualified Data.Map as DM
import Control.Monad
import Data.List

workdir = "workdata/"

batchSize = 20000

readFromPipes::[String]->Consumer [String] IO ()
readFromPipes head = do
	body <- await
	lift $ append2FileLIst workdir head body
	readFromPipes head


append2FileLIst dir fNameList dataList = do
	zipWithM_ (\x y -> appendFile (dir++x ::FilePath) y) fNameList dataList

main = do 
	s<- getArgs
	let num =  (1+) $ (\x -> div x batchSize)  $ (read . head) s 
	let readCsv = splitCSV 
	withFile "data/train_rev2" ReadMode $ \h -> do 
		csvHead <- hGetLine h 
		runEffect $ 
			readFileBatch h batchSize readCsv  ( (map unlines) . transpose) 
			>-> P.take num >-> readFromPipes (readCsv csvHead)
--		let outList = zip  (readCsv csvHead)  $ map DM.toList t 
--		mapM (\x -> appendFile "testdata"  ((show x)++ "\n" ) ) outList