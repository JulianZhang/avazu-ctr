import System.IO
import System.Directory
import System.Environment
import Text.CSV.Lazy.String

import Data.List
import Control.Monad


loadFiles dir wdir = do
	fileList <- fmap tail $ fmap tail $ getDirectoryContents dir
	stringList <- mapM (readFile2Lines dir ) fileList
	let tblList = map pCSV2Table stringList
	let countList = map countProb tblList
	--return  countList
	zipWithM_ (\x y -> writeFile (wdir++x) (show y) ) fileList countList

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



main = loadFiles "./mapCount/" "./workdata/"




