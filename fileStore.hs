import Util.Util
import System.IO

import System.Environment

import Pipes
import qualified Pipes.Prelude as P

import qualified Data.Map as DM
import Control.Monad



append2FileLIst dir fNameList dataList = do

	zipWithM_ (\x y -> appendFile (dir++x ::FilePath) y) fNameList dataList

main = do
 return "" 
--	s<- getArgs
--	let num =  (1+) $ (\x -> div x splitLength)  $ (read . head) s 
--	let readCsv = splitCSV 
--	withFile "data/test_rev2" ReadMode $ \h -> do 
--		csvHead <- hGetLine h 
--		t <- runEffect $ 
--			readFileBatch h 50 readCsv  transpose 
--			>-> P.take num
--		let outList = zip  (readCsv csvHead)  $ map DM.toList t 
--		mapM (\x -> appendFile "testdata"  ((show x)++ "\n" ) ) outList