
import Data.Ord
import Text.CSV.Lazy.String
import Data.List
import System.IO
import Data.Function (on)

splitLength = 5000

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

foldData::Ord a=> [(a,Int)]->[(a,Int)]->[(a,Int)]
foldData lxs rxs = map combind wlist
	where
		wlist = groupBy ((==) `on` fst) $ sortBy (comparing fst) $ lxs ++ rxs
		combind xs 
		 | 1==(length xs) = head xs
		 | 2 ==(length xs) = (((fst . head) xs ), ((snd . head) xs)+((snd . last) xs))


loadTestData = do
	testFile <- readFile "data/test_rev2"
	let cfile = fromCSVTable $ csvTable $ parseCSV testFile
	let coloum = head cfile
	let body =  tail cfile
	let countData = foldl1 (zipWith  foldData)  $ map  getCountData  $ mySplit' body 
	let output =  zip coloum  $ map ( reverse . sortBy (comparing snd) ) countData
	--mapM (\(file, x) -> appendFile  (FilePath file)  ((show x) ++ "\n") ) output
	appendFile "testdata" $ foldl1 (\x y -> x ++"\n"++y)$ map show $tail output
	--mapM (\(_,x) -> appendFile "testdata" ( (show x) ++ "\n") )  countData
	
