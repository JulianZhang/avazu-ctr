
import System.IO
import System.Directory

import Data.List
import Control.Monad

import Pipes
import qualified Pipes.Prelude as P

import qualified Data.Map.Strict as DM
import Data.Fixed (div')
import Util.Util

cleanResult rFile output num = do
	hOutput <- openFile output AppendMode
	hRFile <- openFile rFile ReadMode  
	runEffect $ ( P.fromHandle hRFile) >-> P.take num >->
		P.map splitCSV' >-> P.map cleanResultStep  >-> P.toHandle hOutput    
	hClose hOutput
	hClose hRFile

cleanResultStep strlist =  id ++ "," ++  lv
	where
		id = head strlist
		value = ((read . last) strlist)::Float
		lv =  show $ (\x -> div' x 1.0) $ logBase 10 value 

-- main = cleanResult "./workdata/test_re1" "./workdata/re_lv" 4769401

countResult rFile output num = do 
	hOutput <- openFile output AppendMode
	hRFile <- openFile rFile ReadMode  
	outData <- P.fold (\x y -> DM.unionWith (+) x y) emptyMap id $ 
		( P.fromHandle hRFile) >-> P.take num >->
		P.map splitCSV' >-> P.map last >-> P.map (\x -> DM.singleton x 1)
	hPutStr hOutput  $ mapToString  outData   
	hClose hOutput
	hClose hRFile

mapToString m = unlines $ map eachItem itemList
  where 
    itemList = DM.toList m
    eachItem (x,i) = show x ++ ","  ++ show i 

emptyMap = DM.empty::(DM.Map String Int)

-- main = countResult "./workdata/re_lv" "./workdata/re_count" 4769401

finalResult rFile output num = do 
	hOutput <- openFile output AppendMode
	hRFile <- openFile rFile ReadMode  
	runEffect $ ( P.fromHandle hRFile) >-> P.take num >->
		P.map splitCSV' >-> P.map finalResultStep  >-> P.toHandle hOutput    
	hClose hOutput
	hClose hRFile

finalResultStep strlist = id ++ "," ++ ft value
	where
	id = head strlist
	value = (read . last) strlist

ft v 
	| v > -14 = "1"
	| v == -14 = "0.5"	
	| otherwise = "0"

main = finalResult "./workdata/re_lv" "./workdata/ft2" 4577464
	-- countResult "./workdata/re_lv" "./workdata/re_count" 4577464
--cleanResult "./workdata/newtest" "./workdata/re_lv" 4577464 >> finalResult "./workdata/re_lv" "./workdata/ft" 4577464