import System.IO
import System.Directory
import System.Environment
import Text.CSV.Lazy.String


loadFiles dir = do
	fileList <- fmap tail $ fmap tail $ getDirectoryContents dir
	stringList <- mapM (readFile2Lines dir ) fileList
	let tblList = map pCSV2Table stringList
	return $ head tblList

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

--countProb strList

