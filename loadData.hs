
import Data.Ord
import Text.CSV.Lazy.String
import Data.List
import System.IO

splitLength = 50

workdata = "workdata/"

mySplit' [] = []
mySplit' xs = [x] ++  mySplit' t 
	where
	x = take splitLength xs 
	t = drop splitLength xs                

--mySplit::[a]->[(int,[a])]
mySplit xs = zip [1..] ( mySplit' xs)

getCountData::Ord a => [[a]] -> [[(a,Int)]]
getCountData t =  map ( reverse . sortBy (comparing snd)) $ map 
	(map (\x -> ((head x),length x))) $ 
	map group $ map sort $ transpose t



loadTestData = do
	testFile <- readFile "data/test_rev2"
	let cfile = fromCSVTable $ csvTable $ parseCSV testFile
	let coloum = head cfile
	let body = take 1000 $ tail cfile
	let countData = map  getCountData  $ mySplit' body 
	let output =  map (zip coloum) countData
	mapM (\(file, x) -> appendFile  (FilePath file)  ((show x) ++ "\n") ) output
	--return output
	--mapM (\(_,x) -> appendFile "testdata" ( (show x) ++ "\n") )  countData
	
