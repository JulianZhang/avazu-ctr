import System.IO
import System.Directory
import System.Environment
import Text.CSV.Lazy.String

import Data.List
import Control.Monad

import Pipes
import qualified Pipes.Prelude as P

import Util.Util

import qualified Data.Map.Strict as DM


loadFiles dir wdir = do
	fileList <- fmap tail $ fmap tail $ getDirectoryContents dir
	stringList <- mapM (readFile2Lines dir ) fileList
	let tblList = map pCSV2Table stringList
	let countList =  map countProb tblList
	--return  countList
	zipWithM_ (\x y -> writeFile (wdir++x) (prob2str y) ) fileList countList

prob2str p = unlines $ map prob2str' p
	
prob2str' ((a,b),(c,d)) = (show a) ++ "," ++ (show b) ++ "," ++ (show c) ++ "," ++ (show d)

readFile2Lines dir filename =  readFile (dir++filename)

pCSV2Table str =  filter filterNull $ map list2Tuple strTable
	where 
	strTable = fromCSVTable $ csvTable $  parseCSV str

filterNull (x,y,_) = not $ or [(x == ""),(y == "")]


list2Tuple l = (key,flag,value)
	where
	key = head l
	value = last l
	flag = head $ tail l 

getKey (x,_,_) =x
getFlag (_,y,_) = y
getValue (_,_,z) = read z
getKF (x,y,_) = (x,y)

countProb::Ord a=>[(a,a,String)] -> [((a,a),(Double,Double))]
countProb pList = zipWith getProb kflist kfcount
	where
		(keyList,keyCount) = getCount getKey pList
		(flagList,flagCount) = getCount getFlag pList
		(kflist,kfcount) = getCount getKF pList
		all = sum keyCount
		getKeyCount k  = keyCount!!ki
			where
				ki = head $ elemIndices k keyList
		getFlagCount f  = flagCount!!fi
			where
				fi = head $ elemIndices f flagList
		getProb (k,f) v = ((k,f),(( (v+1)/(kc+1) ),kc))
			where
				kc = getKeyCount k
				fc = getFlagCount f 


getCount getFun inList = (klist,kcount)
	where 
		klist = map head $ group $ sort $ map getFun inList
		kcount = map sum $ map (map getValue) 
			$  map (\x -> filter (\y ->  x == getFun y ) inList) klist

testProb headFile probDir testFile output = do
	headList <- fmap (head . fromCSVTable . csvTable . parseCSV ) $ readFile headFile
	let idStr = head headList
	let colList  = tail headList
	probList <- getProbList colList probDir
	hOutput <- openFile output AppendMode
	hTest <- openFile testFile ReadMode  
	--testList <- fmap (tail . fromCSVTable . csvTable . parseCSV ) $ readFile testFile -- fist row is not data
	--fmap putStrLn $ fmap show $ fmap (testProbAll testList) probList
	runEffect $ ( P.fromHandle hTest) >-> P.drop 1 >-> P.take 1000000000 >->
		P.map splitCSV' >-> P.map (testProbStep probList) >-> P.toHandle hOutput    
	hClose hOutput

	--P.map (fmap testProbStep probList) $ P.map  splitCSV' 

testProbAll testList probList = map (testProbStep probList) testList

testProbStep probList testStrs = (show id) ++ "," ++ (show probData)
	where
		id = head  testStrs
		value = tail testStrs
		probData = foldl' (*) 1 $ zipWith (\f d -> f d) probList value

getProbList colList probDir = sequence $ map (getProb probDir) colList

getProb probDir colStr = do
	let filePath = probDir ++ "/" ++ colStr ++"_count" -- same tag in fileStore
	--putStrLn filePath
	flag <- doesFileExist filePath
	case flag of 
		False -> return (\_ -> 1.0)
		True ->  getProb' filePath

--getprob'::String->[((String,String),(Double,Double))]
getProb' filePath = do
	pd <-  fmap (fromCSVTable . csvTable . parseCSV )$ readFile filePath
	return $ findProb pd

findProb::[[String]]->String->Double
findProb pd key = findProb''$ DM.findWithDefault (defaultPrb,10000.0) (key,"1") keyMap 
	where
	kf =  filter (\x ->and [(key == (x!!0) ),("1"== (x!!1) )] ) pd
	keyMap = DM.fromList $ map list2map pd

list2map::[String]->((String,String),(Double,Double) )
list2map pl = ((pv,tag),(value,count) )
	where 
		pv = head pl 
		tag = head $ tail pl 
		value = read $ head $ tail $ tail pl  
		count = read $ last pl



findProb'' (prob,count)
	| count > countT = prob
	| otherwise = defaultPrb

main = do
	-- loadFiles "./mapCount/" "./probData/"
	testProb "./data2/test.header" "./probData/" "./data2/test"  "./workdata/newtest2"

defaultPrb::Double
defaultPrb =   6865046 / (33563854 + 6865046)

countT = 500.0
